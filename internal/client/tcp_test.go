package client

import (
	"context"
	"net"
	"testing"

	"github.com/CallMeWasabi/socket-go/internal/protocol"
)

func TestClientDialSendAndReadMessage(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		processor := protocol.NewFrameProcessor()
		for {
			frame, err := protocol.ReadFrame(conn)
			if err != nil {
				serverDone <- err
				return
			}
			if err := processor.Record(frame); err != nil {
				serverDone <- err
				return
			}
			message, err := processor.Build()
			if err == protocol.ErrIncompleteFrame {
				continue
			}
			if err != nil {
				serverDone <- err
				return
			}
			if message.Method != "PING" {
				serverDone <- context.Canceled
				return
			}
			response := protocol.NewFrame(protocol.MethodType, message.Channel)
			if err := response.WriteString("PONG"); err != nil {
				serverDone <- err
				return
			}
			for _, outgoing := range []protocol.Frame{
				*protocol.NewFrame(protocol.HeaderType, message.Channel),
				*response,
				*protocol.NewFrame(protocol.EndType, message.Channel),
			} {
				if _, err := outgoing.WriteTo(conn); err != nil {
					serverDone <- err
					return
				}
			}
			processor.Reset()
		}
	}()

	conn, err := Dial(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.SendMessage(
		*protocol.NewFrame(protocol.HeaderType, 2),
		*frameWithBody(protocol.MethodType, 2, "PING"),
		*protocol.NewFrame(protocol.EndType, 2),
	); err != nil {
		t.Fatal(err)
	}

	message, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if message.Method != "PONG" {
		t.Fatalf("method = %q, want PONG", message.Method)
	}
}

func frameWithBody(t protocol.FrameType, channel uint8, body string) *protocol.Frame {
	frame := protocol.NewFrame(t, channel)
	if err := frame.WriteString(body); err != nil {
		panic(err)
	}
	return frame
}
