package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/CallMeWasabi/socket-go/internal/client"
	"github.com/CallMeWasabi/socket-go/internal/protocol"
)

func TestServerClientRoundTrip(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan protocol.FullFrame, 1)
	handler := HandlerFunc(func(ctx context.Context, session *Session, message protocol.FullFrame) error {
		received <- message
		response := protocol.NewFrame(protocol.MethodType, message.Channel)
		if err := response.WriteString("OK"); err != nil {
			return err
		}
		return session.SendMessage(
			*protocol.NewFrame(protocol.HeaderType, message.Channel),
			*response,
			*protocol.NewFrame(protocol.EndType, message.Channel),
		)
	})

	server := NewTCPServer(listener, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()

	conn, err := client.Dial(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.SendMessage(
		*protocol.NewFrame(protocol.HeaderType, 1),
		*frameWithBody(protocol.MethodType, 1, "PING"),
		*frameWithBody(protocol.BodyType, 1, "hello"),
		*protocol.NewFrame(protocol.EndType, 1),
	); err != nil {
		t.Fatal(err)
	}

	select {
	case message := <-received:
		if message.Method != "PING" || message.Body.String() != "hello" {
			t.Fatalf("message = %#v, want PING/hello", message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server handler")
	}

	response, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if response.Method != "OK" {
		t.Fatalf("response method = %q, want OK", response.Method)
	}

	_ = server.Close()
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}

func frameWithBody(t protocol.FrameType, channel uint8, body string) *protocol.Frame {
	frame := protocol.NewFrame(t, channel)
	if err := frame.WriteString(body); err != nil {
		panic(err)
	}
	return frame
}
