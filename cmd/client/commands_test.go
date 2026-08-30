package main

import (
	"bytes"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	tcpclient "github.com/CallMeWasabi/socket-go/internal/client"
	"github.com/CallMeWasabi/socket-go/internal/protocol"
	"github.com/google/uuid"
)

func TestParsePublishPreservesPayloadSpaces(t *testing.T) {
	command, err := parseCommand("publish orders hello world")
	if err != nil {
		t.Fatal(err)
	}
	if command.kind != commandPublish || command.topic != "orders" || command.payload != "hello world" {
		t.Fatalf("command = %#v, want publish orders hello world", command)
	}
}

func TestBuildSubscribeFrames(t *testing.T) {
	command, err := parseCommand("subscribe orders")
	if err != nil {
		t.Fatal(err)
	}

	frames, err := buildFrames(command, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 || frames[0].Type != protocol.HeaderType || frames[1].Type != protocol.MethodType || frames[2].Type != protocol.EndType {
		t.Fatalf("frame sequence = %#v, want Header/Method/End", frames)
	}
	if string(frames[1].Body[:frames[1].Length]) != "SUBSCRIBE" {
		t.Fatalf("method = %q, want SUBSCRIBE", frames[1].Body[:frames[1].Length])
	}
}

func TestBuildPublishFramesIncludesPayload(t *testing.T) {
	command, err := parseCommand("publish orders hello")
	if err != nil {
		t.Fatal(err)
	}

	frames, err := buildFrames(command, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 4 || frames[2].Type != protocol.BodyType {
		t.Fatalf("frame sequence = %#v, want Header/Method/Body/End", frames)
	}
	if string(frames[2].Body[:frames[2].Length]) != "hello" {
		t.Fatalf("payload = %q, want hello", frames[2].Body[:frames[2].Length])
	}
}

func TestParseCommandRejectsMissingArguments(t *testing.T) {
	for _, input := range []string{"publish", "subscribe", "unsubscribe", "unknown command"} {
		if _, err := parseCommand(input); !errors.Is(err, errInvalidCommand) {
			t.Fatalf("parseCommand(%q) error = %v, want %v", input, err, errInvalidCommand)
		}
	}
}

func TestBuildPublishRejectsOversizedPayload(t *testing.T) {
	command := command{kind: commandPublish, topic: "orders", payload: string(make([]byte, protocol.MaxBodySize+1))}
	if _, err := buildFrames(command, 1); !errors.Is(err, protocol.ErrBodyTooLarge) {
		t.Fatalf("buildFrames() error = %v, want %v", err, protocol.ErrBodyTooLarge)
	}
}

func TestRunCLIUsesTCPClientForRemoteCommand(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	received := make(chan error, 1)
	go func() {
		processor := protocol.NewFrameProcessor()
		for {
			frame, err := protocol.ReadFrame(serverConn)
			if err != nil {
				received <- err
				return
			}
			if err := processor.Record(frame); err != nil {
				received <- err
				return
			}
			message, err := processor.Build()
			if errors.Is(err, protocol.ErrIncompleteFrame) {
				continue
			}
			if err != nil {
				received <- err
				return
			}
			if message.Method != "PING" || message.Channel != 1 {
				received <- errors.New("unexpected CLI request")
				return
			}
			received <- nil
			return
		}
	}()

	var output bytes.Buffer
	err := runCLI(tcpclient.New(clientConn), strings.NewReader("help\nping\nexit\n"), &output)
	if err != nil {
		t.Fatal(err)
	}
	_ = clientConn.Close()
	if err := <-received; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Commands:") {
		t.Fatalf("output = %q, want help text", output.String())
	}
}

func TestHandleReaderAutomaticallyAcksMessage(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	readerDone := make(chan struct{})
	var output bytes.Buffer
	go func() {
		handleReader(tcpclient.New(clientConn), &output)
		close(readerDone)
	}()

	messageHeader := protocol.NewFrame(protocol.HeaderType, 1)
	if err := messageHeader.WriteString("topic=orders\ndelivery-id=" + uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	messageMethod := protocol.NewFrame(protocol.MethodType, 1)
	if err := messageMethod.WriteString("MESSAGE"); err != nil {
		t.Fatal(err)
	}
	messageBody := protocol.NewFrame(protocol.BodyType, 1)
	if err := messageBody.WriteString("hello"); err != nil {
		t.Fatal(err)
	}
	for _, frame := range []protocol.Frame{*messageHeader, *messageMethod, *messageBody, *protocol.NewFrame(protocol.EndType, 1)} {
		if _, err := frame.WriteTo(serverConn); err != nil {
			t.Fatal(err)
		}
	}

	processor := protocol.NewFrameProcessor()
	for {
		frame, err := protocol.ReadFrame(serverConn)
		if err != nil {
			t.Fatal(err)
		}
		if err := processor.Record(frame); err != nil {
			t.Fatal(err)
		}
		ack, err := processor.Build()
		if errors.Is(err, protocol.ErrIncompleteFrame) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if ack.Method != "ACK" {
			t.Fatalf("auto response method = %q, want ACK", ack.Method)
		}
		if ack.Header["delivery-id"] != messageHeaderBody(messageHeader, "delivery-id") {
			t.Fatalf("ack delivery-id = %q, want original", ack.Header["delivery-id"])
		}
		break
	}
	_ = serverConn.Close()
	select {
	case <-readerDone:
	case <-time.After(time.Second):
		t.Fatal("reader did not stop")
	}
}

func messageHeaderBody(frame *protocol.Frame, key string) string {
	parts := strings.Split(string(frame.Body[:frame.Length]), "\n")
	for _, part := range parts {
		if strings.HasPrefix(part, key+"=") {
			return strings.TrimPrefix(part, key+"=")
		}
	}
	return ""
}
