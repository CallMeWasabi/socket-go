package main

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CallMeWasabi/socket-go/internal/client"
	"github.com/CallMeWasabi/socket-go/internal/core"
	"github.com/CallMeWasabi/socket-go/internal/protocol"
	"github.com/CallMeWasabi/socket-go/internal/server"
	"github.com/google/uuid"
)

func TestTCPPublishSubscribeAckFlow(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	exchange := core.NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exchange.Run(ctx)

	states := &sync.Map{}
	tcpServer := server.NewTCPServer(listener, newServerHandler(exchange, states))
	tcpServer.SetSessionCloseHandler(func(session *server.Session) {
		if value, ok := states.LoadAndDelete(session.SessionID()); ok {
			cleanupSession(exchange, value.(*sessionState))
		}
	})
	serveDone := make(chan error, 1)
	go func() { serveDone <- tcpServer.Serve(ctx) }()

	subscriber, err := client.Dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := client.Dial(ctx, listener.Addr().String())
	if err != nil {
		_ = subscriber.Close()
		t.Fatal(err)
	}

	defer func() {
		_ = subscriber.Close()
		_ = publisher.Close()
		_ = tcpServer.Close()
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	}()

	if err := subscriber.SendMessage(requestFrames(1, "SUBSCRIBE", "orders", nil)...); err != nil {
		t.Fatal(err)
	}
	response, err := subscriber.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if response.Method != "SUBSCRIBE_OK" {
		t.Fatalf("subscribe response = %q, want SUBSCRIBE_OK", response.Method)
	}

	if err := publisher.SendMessage(requestFrames(2, "PUBLISH", "orders", []byte("hello"))...); err != nil {
		t.Fatal(err)
	}
	response, err = publisher.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if response.Method != "PUBLISH_OK" {
		t.Fatalf("publish response = %q, want PUBLISH_OK", response.Method)
	}

	delivery, err := subscriber.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Method != "MESSAGE" || delivery.Body.String() != "hello" {
		t.Fatalf("delivery = method=%q body=%q", delivery.Method, delivery.Body.String())
	}
	deliveryID := delivery.Header["delivery-id"]
	if _, err := uuid.Parse(deliveryID); err != nil {
		t.Fatalf("delivery-id = %q is invalid: %v", deliveryID, err)
	}

	if err := subscriber.SendMessage(ackFrames(1, delivery.Header["topic"], deliveryID)...); err != nil {
		t.Fatal(err)
	}
	response, err = subscriber.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if response.Method != "ACK_OK" {
		t.Fatalf("ack response = %q, want ACK_OK", response.Method)
	}
}

func requestFrames(channel uint8, method, topic string, body []byte) []protocol.Frame {
	header := protocol.NewFrame(protocol.HeaderType, channel)
	_ = header.WriteString("request-id=" + uuid.NewString() + "\ntopic=" + topic)
	methodFrame := protocol.NewFrame(protocol.MethodType, channel)
	_ = methodFrame.WriteString(method)
	frames := []protocol.Frame{*header, *methodFrame}
	if len(body) > 0 {
		bodyFrame := protocol.NewFrame(protocol.BodyType, channel)
		_ = bodyFrame.SetBody(body)
		frames = append(frames, *bodyFrame)
	}
	return append(frames, *protocol.NewFrame(protocol.EndType, channel))
}

func ackFrames(channel uint8, topic, deliveryID string) []protocol.Frame {
	header := protocol.NewFrame(protocol.HeaderType, channel)
	_ = header.WriteString(strings.Join([]string{
		"request-id=" + uuid.NewString(),
		"topic=" + topic,
		"delivery-id=" + deliveryID,
	}, "\n"))
	method := protocol.NewFrame(protocol.MethodType, channel)
	_ = method.WriteString("ACK")
	return []protocol.Frame{*header, *method, *protocol.NewFrame(protocol.EndType, channel)}
}
