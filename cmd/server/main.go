package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CallMeWasabi/socket-go/internal/core"
	"github.com/CallMeWasabi/socket-go/internal/protocol"
	"github.com/CallMeWasabi/socket-go/internal/server"
	"github.com/google/uuid"
)

const consumerBufferSize = 32

type sessionState struct {
	process *core.Process

	mu            sync.Mutex
	wireChannels  map[uint8]uuid.UUID
	subscriptions map[uuid.UUID]map[string]struct{}
	forwarders    map[uuid.UUID]struct{}
}

func newSessionState(session *server.Session) *sessionState {
	process := core.NewProcess(session.Conn)
	process.ID = session.SessionID()
	return &sessionState{
		process:       process,
		wireChannels:  make(map[uint8]uuid.UUID),
		subscriptions: make(map[uuid.UUID]map[string]struct{}),
		forwarders:    make(map[uuid.UUID]struct{}),
	}
}

func (s *sessionState) channel(session *server.Session, wireID uint8, channelType uint8) (core.Channel, error) {
	id := channelID(session.SessionID(), wireID)
	if existing, ok := s.process.LookupChannel(id); ok {
		if existing.Type != channelType {
			return core.Channel{}, fmt.Errorf("channel %d already has another type", wireID)
		}
		s.mu.Lock()
		s.wireChannels[wireID] = id
		s.mu.Unlock()
		return existing, nil
	}

	channel := core.Channel{
		ID:         id,
		ConsumerID: session.SessionID(),
		Type:       channelType,
	}
	if channelType == core.ConsumerChannel {
		channel.OutBuffer = make(chan *core.DeliveryMessage, consumerBufferSize)
	}
	if err := s.process.RegisterChannel(channel); err != nil {
		return core.Channel{}, err
	}
	s.mu.Lock()
	s.wireChannels[wireID] = id
	s.mu.Unlock()
	return channel, nil
}

func (s *sessionState) addSubscription(channelID uuid.UUID, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscriptions[channelID] == nil {
		s.subscriptions[channelID] = make(map[string]struct{})
	}
	s.subscriptions[channelID][topic] = struct{}{}
}

func (s *sessionState) removeSubscription(channelID uuid.UUID, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if topics := s.subscriptions[channelID]; topics != nil {
		delete(topics, topic)
		if len(topics) == 0 {
			delete(s.subscriptions, channelID)
		}
	}
}

func (s *sessionState) startForwarder(session *server.Session, channel core.Channel, wireID uint8) {
	s.mu.Lock()
	if _, started := s.forwarders[channel.ID]; started {
		s.mu.Unlock()
		return
	}
	s.forwarders[channel.ID] = struct{}{}
	s.mu.Unlock()

	go func() {
		for delivery := range channel.OutBuffer {
			frames, err := deliveryFrames(wireID, delivery)
			if err != nil {
				_ = session.Close()
				return
			}
			if err := session.SendMessage(frames...); err != nil {
				_ = session.Close()
				return
			}
		}
	}()
}

func main() {
	exchange := core.NewExchange()
	ctx := context.Background()
	go func() {
		if err := exchange.Run(ctx); err != nil {
			log.Println("exchange stopped:", err)
		}
	}()

	states := &sync.Map{}
	handler := newServerHandler(exchange, states)

	tcpServer, err := server.Listen(":8080", handler)
	if err != nil {
		log.Fatal("listen failed:", err)
	}
	tcpServer.SetSessionCloseHandler(func(session *server.Session) {
		value, ok := states.LoadAndDelete(session.SessionID())
		if !ok {
			return
		}
		cleanupSession(exchange, value.(*sessionState))
	})
	defer tcpServer.Close()

	log.Printf("listening on %s", tcpServer.Addr())
	if err := tcpServer.Serve(ctx); err != nil {
		log.Fatal("server failed:", err)
	}
}

func newServerHandler(exchange *core.Exchange, states *sync.Map) server.Handler {
	return server.HandlerFunc(func(ctx context.Context, session *server.Session, message protocol.FullFrame) error {
		value, loaded := states.Load(session.SessionID())
		var state *sessionState
		if loaded {
			state = value.(*sessionState)
		} else {
			state = newSessionState(session)
			states.Store(session.SessionID(), state)
		}
		return handleRequest(ctx, exchange, session, state, message)
	})
}

func handleRequest(ctx context.Context, exchange *core.Exchange, session *server.Session, state *sessionState, message protocol.FullFrame) error {
	topic := strings.TrimSpace(message.Header["topic"])
	requestID := message.Header["request-id"]

	switch message.Method {
	case "SUBSCRIBE":
		if topic == "" {
			return sendRequestError(session, message.Channel, requestID, "topic is required")
		}
		channel, err := state.channel(session, message.Channel, core.ConsumerChannel)
		if err == nil {
			err = exchange.Subscribe(ctx, topic, channel)
		}
		if err != nil {
			return sendRequestError(session, message.Channel, requestID, err.Error())
		}
		state.addSubscription(channel.ID, topic)
		state.startForwarder(session, channel, message.Channel)
		return sendResponse(session, message.Channel, "SUBSCRIBE_OK", requestID, nil)

	case "UNSUBSCRIBE":
		if topic == "" {
			return sendRequestError(session, message.Channel, requestID, "topic is required")
		}
		channel, ok := state.process.LookupChannel(channelID(session.SessionID(), message.Channel))
		if !ok || channel.Type != core.ConsumerChannel {
			return sendRequestError(session, message.Channel, requestID, "consumer channel is not registered")
		}
		if err := exchange.Unsubscribe(ctx, topic, channel.ID); err != nil {
			return sendRequestError(session, message.Channel, requestID, err.Error())
		}
		state.removeSubscription(channel.ID, topic)
		return sendResponse(session, message.Channel, "UNSUBSCRIBE_OK", requestID, nil)

	case "PUBLISH":
		if topic == "" {
			return sendRequestError(session, message.Channel, requestID, "topic is required")
		}
		channel, err := state.channel(session, message.Channel, core.PublisherChannel)
		if err == nil {
			err = exchange.PublishFrom(ctx, topic, channel.ID, &core.RawMessage{Topic: topic, Content: message.Body.String(), PublisherChannelID: channel.ID})
		}
		if err != nil {
			return sendRequestError(session, message.Channel, requestID, err.Error())
		}
		return sendResponse(session, message.Channel, "PUBLISH_OK", requestID, nil)

	case "ACK", "NACK":
		if topic == "" {
			return sendRequestError(session, message.Channel, requestID, "topic is required")
		}
		deliveryID, err := uuid.Parse(strings.TrimSpace(message.Header["delivery-id"]))
		if err != nil {
			return sendRequestError(session, message.Channel, requestID, "invalid delivery-id")
		}
		channel, ok := state.process.LookupChannel(channelID(session.SessionID(), message.Channel))
		if !ok || channel.Type != core.ConsumerChannel {
			return sendRequestError(session, message.Channel, requestID, "consumer channel is not registered")
		}
		var ackErr error
		if message.Method == "ACK" {
			ackErr = exchange.Ack(ctx, topic, channel.ID, deliveryID)
		} else {
			ackErr = exchange.Nack(ctx, topic, channel.ID, deliveryID)
		}
		if ackErr != nil {
			return sendRequestError(session, message.Channel, requestID, ackErr.Error())
		}
		return sendResponse(session, message.Channel, message.Method+"_OK", requestID, nil)

	case "TOPICS":
		topics, err := exchange.Topics(ctx)
		if err != nil {
			return sendRequestError(session, message.Channel, requestID, err.Error())
		}
		return sendResponse(session, message.Channel, "TOPICS_OK", requestID, []byte(strings.Join(topics, "\n")))

	case "PING":
		return sendResponse(session, message.Channel, "PONG", requestID, nil)

	case "EXIT":
		if err := sendResponse(session, message.Channel, "BYE", requestID, nil); err != nil {
			return err
		}
		_ = session.Close()
		return nil

	default:
		return sendRequestError(session, message.Channel, requestID, "unknown method")
	}
}

func cleanupSession(exchange *core.Exchange, state *sessionState) {
	state.mu.Lock()
	subscriptions := make(map[uuid.UUID][]string, len(state.subscriptions))
	for channelID, topics := range state.subscriptions {
		for topic := range topics {
			subscriptions[channelID] = append(subscriptions[channelID], topic)
		}
	}
	state.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for channelID, topics := range subscriptions {
		for _, topic := range topics {
			_ = exchange.Unsubscribe(ctx, topic, channelID)
		}
	}
	for _, channel := range state.process.ChannelsSnapshot() {
		if channel.OutBuffer != nil {
			close(channel.OutBuffer)
		}
		_, _ = state.process.RemoveChannel(channel.ID)
	}
}

func deliveryFrames(wireChannel uint8, delivery *core.DeliveryMessage) ([]protocol.Frame, error) {
	headers := map[string]string{
		"topic":       delivery.Meta.Topic,
		"message-id":  delivery.Meta.MessageID.String(),
		"delivery-id": delivery.Meta.DeliveryID.String(),
		"attempt":     strconv.FormatInt(delivery.Meta.Attempt, 10),
	}
	return responseFrames(wireChannel, "MESSAGE", headers, delivery.Content)
}

func sendResponse(session *server.Session, channel uint8, method, requestID string, body []byte) error {
	headers := map[string]string{}
	if requestID != "" {
		headers["request-id"] = requestID
	}
	frames, err := responseFrames(channel, method, headers, body)
	if err != nil {
		return err
	}
	return session.SendMessage(frames...)
}

func sendRequestError(session *server.Session, channel uint8, requestID, message string) error {
	if err := sendResponse(session, channel, "ERROR", requestID, []byte(message)); err != nil {
		return err
	}
	return nil
}

func responseFrames(channel uint8, method string, headers map[string]string, body []byte) ([]protocol.Frame, error) {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, key+"="+headers[key])
	}
	header := protocol.NewFrame(protocol.HeaderType, channel)
	if err := header.WriteString(strings.Join(lines, "\n")); err != nil {
		return nil, err
	}
	methodFrame := protocol.NewFrame(protocol.MethodType, channel)
	if err := methodFrame.WriteString(method); err != nil {
		return nil, err
	}
	frames := []protocol.Frame{*header, *methodFrame}
	for len(body) > 0 {
		n := len(body)
		if n > protocol.MaxBodySize {
			n = protocol.MaxBodySize
		}
		part := protocol.NewFrame(protocol.BodyType, channel)
		if err := part.SetBody(body[:n]); err != nil {
			return nil, err
		}
		frames = append(frames, *part)
		body = body[n:]
	}
	frames = append(frames, *protocol.NewFrame(protocol.EndType, channel))
	return frames, nil
}

func channelID(sessionID uuid.UUID, channel uint8) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(sessionID.String()+":"+strconv.Itoa(int(channel))))
}
