package main

import (
	"errors"
	"strings"

	"github.com/CallMeWasabi/socket-go/internal/protocol"
	"github.com/google/uuid"
)

type commandKind uint8

const (
	commandHelp commandKind = iota
	commandPublish
	commandSubscribe
	commandUnsubscribe
	commandTopics
	commandPing
	commandExit
)

var errInvalidCommand = errors.New("invalid command")

type command struct {
	kind    commandKind
	topic   string
	payload string
}

func parseCommand(input string) (command, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return command{}, errInvalidCommand
	}

	name, rest, hasRest := strings.Cut(input, " ")
	name = strings.ToLower(name)
	rest = strings.TrimSpace(rest)

	switch name {
	case "help":
		if hasRest {
			return command{}, errInvalidCommand
		}
		return command{kind: commandHelp}, nil
	case "topics":
		if hasRest {
			return command{}, errInvalidCommand
		}
		return command{kind: commandTopics}, nil
	case "ping":
		if hasRest {
			return command{}, errInvalidCommand
		}
		return command{kind: commandPing}, nil
	case "exit", "quit":
		if hasRest {
			return command{}, errInvalidCommand
		}
		return command{kind: commandExit}, nil
	case "subscribe", "sub":
		return parseTopicCommand(commandSubscribe, rest)
	case "unsubscribe", "unsub":
		return parseTopicCommand(commandUnsubscribe, rest)
	case "publish", "pub":
		topic, payload, ok := strings.Cut(rest, " ")
		payload = strings.TrimSpace(payload)
		if !ok || topic == "" || payload == "" || strings.ContainsAny(topic, " \t\r\n") {
			return command{}, errInvalidCommand
		}
		return command{kind: commandPublish, topic: topic, payload: payload}, nil
	default:
		return command{}, errInvalidCommand
	}
}

func parseTopicCommand(kind commandKind, topic string) (command, error) {
	if topic == "" || strings.ContainsAny(topic, " \t\r\n") {
		return command{}, errInvalidCommand
	}
	return command{kind: kind, topic: topic}, nil
}

func buildFrames(cmd command, channel uint8) ([]protocol.Frame, error) {
	if cmd.kind == commandHelp || cmd.kind == commandExit {
		return nil, errInvalidCommand
	}

	method := ""
	switch cmd.kind {
	case commandPublish:
		method = "PUBLISH"
	case commandSubscribe:
		method = "SUBSCRIBE"
	case commandUnsubscribe:
		method = "UNSUBSCRIBE"
	case commandTopics:
		method = "TOPICS"
	case commandPing:
		method = "PING"
	default:
		return nil, errInvalidCommand
	}

	header := protocol.NewFrame(protocol.HeaderType, channel)
	headerBody := "request-id=" + uuid.NewString()
	if cmd.topic != "" {
		headerBody += "\ntopic=" + cmd.topic
	}
	if err := header.WriteString(headerBody); err != nil {
		return nil, err
	}

	methodFrame := protocol.NewFrame(protocol.MethodType, channel)
	if err := methodFrame.WriteString(method); err != nil {
		return nil, err
	}

	frames := []protocol.Frame{*header, *methodFrame}
	if cmd.kind == commandPublish {
		body := protocol.NewFrame(protocol.BodyType, channel)
		if err := body.WriteString(cmd.payload); err != nil {
			return nil, err
		}
		frames = append(frames, *body)
	}
	frames = append(frames, *protocol.NewFrame(protocol.EndType, channel))
	return frames, nil
}

func buildAckFrames(message protocol.FullFrame) ([]protocol.Frame, error) {
	topic := message.Header["topic"]
	deliveryID := message.Header["delivery-id"]
	if topic == "" || deliveryID == "" {
		return nil, errInvalidCommand
	}
	header := protocol.NewFrame(protocol.HeaderType, message.Channel)
	if err := header.WriteString("request-id=" + uuid.NewString() + "\ntopic=" + topic + "\ndelivery-id=" + deliveryID); err != nil {
		return nil, err
	}
	method := protocol.NewFrame(protocol.MethodType, message.Channel)
	if err := method.WriteString("ACK"); err != nil {
		return nil, err
	}
	return []protocol.Frame{*header, *method, *protocol.NewFrame(protocol.EndType, message.Channel)}, nil
}

func commandChannel(cmd command) uint8 {
	if cmd.kind == commandPublish {
		return 2 // publisher channel
	}
	return 1 // consumer/control channel
}

const cliHelp = `Commands:
  publish <topic> <payload>   publish a message
  subscribe <topic>           subscribe to a topic
  unsubscribe <topic>         unsubscribe from a topic
  topics                      list topics
  ping                        send a ping request
  help                        show this help
  exit                        close the connection`
