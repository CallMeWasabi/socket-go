package core

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/google/uuid"
)

var ErrTopicNotFound = errors.New("core: topic not found")

type ExchangeCommandKind uint8

const (
	ExchangeAddTopic ExchangeCommandKind = iota
	ExchangeRemoveTopic
	ExchangeSubscribe
	ExchangeUnsubscribe
	ExchangePublish
	ExchangeAck
	ExchangeNack
	ExchangeListTopics
	ExchangeTopicStats
)

type ExchangeCommand struct {
	Kind       ExchangeCommandKind
	Topic      string
	Channel    Channel
	ChannelID  uuid.UUID
	DeliveryID uuid.UUID
	Payload    *RawMessage
	Reply      chan ExchangeResult
}

type ExchangeResult struct {
	Topics []string
	Stats  TopicStats
	Err    error
}

type Exchange struct {
	ID uuid.UUID

	commands chan ExchangeCommand
	topics   map[string]topicHandle
	config   TopicConfig
}

type ExchangeConfig struct {
	Topic TopicConfig
}

type topicHandle struct {
	topic  *Topic
	cancel context.CancelFunc
}

func NewExchange() *Exchange {
	return NewExchangeWithConfig(ExchangeConfig{Topic: DefaultTopicConfig()})
}

func NewExchangeWithConfig(config ExchangeConfig) *Exchange {
	if config.Topic.PendingCapacity <= 0 || config.Topic.MaxInFlight <= 0 || config.Topic.RetryAfter <= 0 || config.Topic.MaxAttempts <= 0 {
		defaults := DefaultTopicConfig()
		if config.Topic.PendingCapacity <= 0 {
			config.Topic.PendingCapacity = defaults.PendingCapacity
		}
		if config.Topic.MaxInFlight <= 0 {
			config.Topic.MaxInFlight = defaults.MaxInFlight
		}
		if config.Topic.RetryAfter <= 0 {
			config.Topic.RetryAfter = defaults.RetryAfter
		}
		if config.Topic.MaxAttempts <= 0 {
			config.Topic.MaxAttempts = defaults.MaxAttempts
		}
	}
	return &Exchange{
		ID:       uuid.New(),
		commands: make(chan ExchangeCommand),
		topics:   make(map[string]topicHandle),
		config:   config.Topic,
	}
}

func (e *Exchange) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil
		case command := <-e.commands:
			result := e.handle(runCtx, e.topics, command)
			if command.Reply != nil {
				command.Reply <- result
			}
		}
	}
}

func (e *Exchange) handle(ctx context.Context, topics map[string]topicHandle, command ExchangeCommand) ExchangeResult {
	result := ExchangeResult{}
	name := strings.TrimSpace(command.Topic)
	if command.Kind != ExchangeListTopics && name == "" {
		result.Err = ErrInvalidTopicName
		return result
	}

	switch command.Kind {
	case ExchangeAddTopic:
		if name == "" {
			result.Err = ErrInvalidTopicName
			return result
		}
		if _, ok := topics[name]; !ok {
			topic := NewTopicWithConfig(name, e.config)
			topicCtx, cancel := context.WithCancel(ctx)
			topics[name] = topicHandle{topic: topic, cancel: cancel}
			go topic.Run(topicCtx)
		}
	case ExchangeRemoveTopic:
		if name == "" {
			result.Err = ErrInvalidTopicName
			return result
		}
		handle, ok := topics[name]
		if !ok {
			result.Err = ErrTopicNotFound
			return result
		}
		handle.cancel()
		delete(topics, name)
	case ExchangeSubscribe:
		if name == "" {
			result.Err = ErrInvalidTopicName
			return result
		}
		handle, ok := topics[name]
		if !ok {
			topic := NewTopicWithConfig(name, e.config)
			topicCtx, cancel := context.WithCancel(ctx)
			handle = topicHandle{topic: topic, cancel: cancel}
			topics[name] = handle
			go topic.Run(topicCtx)
		}
		_, result.Err = handle.topic.submit(ctx, TopicCommand{Kind: TopicSubscribe, Channel: command.Channel})
	case ExchangeUnsubscribe:
		handle, ok := topics[name]
		if !ok {
			result.Err = ErrTopicNotFound
			return result
		}
		_, result.Err = handle.topic.submit(ctx, TopicCommand{Kind: TopicUnsubscribe, ChannelID: command.ChannelID})
	case ExchangePublish:
		handle, ok := topics[name]
		if !ok {
			result.Err = ErrTopicNotFound
			return result
		}
		if command.Payload != nil {
			payload := *command.Payload
			if payload.PublisherChannelID == uuid.Nil {
				payload.PublisherChannelID = command.ChannelID
			}
			command.Payload = &payload
		}
		_, result.Err = handle.topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: command.Payload})
	case ExchangeAck:
		handle, ok := topics[name]
		if !ok {
			result.Err = ErrTopicNotFound
			return result
		}
		_, result.Err = handle.topic.submit(ctx, TopicCommand{Kind: TopicAck, ChannelID: command.ChannelID, DeliveryID: command.DeliveryID})
	case ExchangeNack:
		handle, ok := topics[name]
		if !ok {
			result.Err = ErrTopicNotFound
			return result
		}
		_, result.Err = handle.topic.submit(ctx, TopicCommand{Kind: TopicNack, ChannelID: command.ChannelID, DeliveryID: command.DeliveryID})
	case ExchangeTopicStats:
		handle, ok := topics[name]
		if !ok {
			result.Err = ErrTopicNotFound
			return result
		}
		topicResult, err := handle.topic.submit(ctx, TopicCommand{Kind: TopicQueryStats})
		result.Stats = topicResult.Stats
		result.Err = err
	case ExchangeListTopics:
		result.Topics = make([]string, 0, len(topics))
		for topic := range topics {
			result.Topics = append(result.Topics, topic)
		}
		sort.Strings(result.Topics)
	default:
		result.Err = errors.New("core: unknown exchange command")
	}
	return result
}

func (e *Exchange) submit(ctx context.Context, command ExchangeCommand) (ExchangeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if command.Reply == nil {
		command.Reply = make(chan ExchangeResult, 1)
	}

	select {
	case e.commands <- command:
	case <-ctx.Done():
		return ExchangeResult{}, ctx.Err()
	}

	select {
	case result := <-command.Reply:
		return result, result.Err
	case <-ctx.Done():
		return ExchangeResult{}, ctx.Err()
	}
}

func (e *Exchange) AddTopic(ctx context.Context, name string) error {
	_, err := e.submit(ctx, ExchangeCommand{Kind: ExchangeAddTopic, Topic: name})
	return err
}

func (e *Exchange) RemoveTopic(ctx context.Context, name string) error {
	_, err := e.submit(ctx, ExchangeCommand{Kind: ExchangeRemoveTopic, Topic: name})
	return err
}

func (e *Exchange) Subscribe(ctx context.Context, name string, channel Channel) error {
	_, err := e.submit(ctx, ExchangeCommand{Kind: ExchangeSubscribe, Topic: name, Channel: channel})
	return err
}

func (e *Exchange) Unsubscribe(ctx context.Context, name string, channelID uuid.UUID) error {
	_, err := e.submit(ctx, ExchangeCommand{Kind: ExchangeUnsubscribe, Topic: name, ChannelID: channelID})
	return err
}

func (e *Exchange) Publish(ctx context.Context, name string, payload *RawMessage) error {
	_, err := e.submit(ctx, ExchangeCommand{Kind: ExchangePublish, Topic: name, Payload: payload})
	return err
}

// PublishFrom publishes a message and records the publisher channel identity.
func (e *Exchange) PublishFrom(ctx context.Context, name string, channelID uuid.UUID, payload *RawMessage) error {
	_, err := e.submit(ctx, ExchangeCommand{Kind: ExchangePublish, Topic: name, ChannelID: channelID, Payload: payload})
	return err
}

// Ack acknowledges a delivery on a consumer channel.
func (e *Exchange) Ack(ctx context.Context, name string, channelID, deliveryID uuid.UUID) error {
	_, err := e.submit(ctx, ExchangeCommand{Kind: ExchangeAck, Topic: name, ChannelID: channelID, DeliveryID: deliveryID})
	return err
}

// Nack rejects a delivery and makes it eligible for immediate redelivery.
func (e *Exchange) Nack(ctx context.Context, name string, channelID, deliveryID uuid.UUID) error {
	_, err := e.submit(ctx, ExchangeCommand{Kind: ExchangeNack, Topic: name, ChannelID: channelID, DeliveryID: deliveryID})
	return err
}

func (e *Exchange) Topics(ctx context.Context) ([]string, error) {
	result, err := e.submit(ctx, ExchangeCommand{Kind: ExchangeListTopics})
	return result.Topics, err
}

func (e *Exchange) Stats(ctx context.Context, name string) (TopicStats, error) {
	result, err := e.submit(ctx, ExchangeCommand{Kind: ExchangeTopicStats, Topic: name})
	return result.Stats, err
}
