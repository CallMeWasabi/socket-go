package core

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidTopicName = errors.New("core: invalid topic name")
	ErrInvalidChannel   = errors.New("core: invalid channel")
	ErrNilPayload       = errors.New("core: nil payload")
	ErrQueueFull        = errors.New("core: topic queue is full")
	ErrDeliveryNotFound = errors.New("core: delivery not found")
	ErrInvalidDelivery  = errors.New("core: delivery does not belong to channel")
)

type TopicCommandKind uint8

const (
	TopicSubscribe TopicCommandKind = iota
	TopicUnsubscribe
	TopicPublish
	TopicAck
	TopicNack
	TopicQueryStats
)

type TopicCommand struct {
	Kind       TopicCommandKind
	Channel    Channel
	ChannelID  uuid.UUID
	DeliveryID uuid.UUID
	Payload    *RawMessage
	Reply      chan TopicResult
}

type TopicResult struct {
	Stats TopicStats
	Err   error
}

type TopicStats struct {
	Name        string
	Subscribers int
	Published   uint64
	Pending     int
	Unacked     int
	Dropped     uint64
}

type TopicConfig struct {
	PendingCapacity int
	MaxInFlight     int
	RetryAfter      time.Duration
	MaxAttempts     int64
}

func DefaultTopicConfig() TopicConfig {
	return TopicConfig{
		PendingCapacity: 128,
		MaxInFlight:     1,
		RetryAfter:      time.Minute,
		MaxAttempts:     3,
	}
}

type Topic struct {
	Name     string
	commands chan TopicCommand
	config   TopicConfig

	// These fields are accessed only by Run and its actor-owned helpers.
	channels  map[uuid.UUID]*Channel
	order     []uuid.UUID
	inFlight  map[uuid.UUID]int
	unacked   map[uuid.UUID]*DeliveryMessage
	pending   []*RawMessage
	rrIdx     int
	published uint64
	dropped   uint64
}

func NewTopic(name string) *Topic {
	return NewTopicWithConfig(name, DefaultTopicConfig())
}

func NewTopicWithConfig(name string, config TopicConfig) *Topic {
	defaults := DefaultTopicConfig()
	if config.PendingCapacity <= 0 {
		config.PendingCapacity = defaults.PendingCapacity
	}
	if config.MaxInFlight <= 0 {
		config.MaxInFlight = defaults.MaxInFlight
	}
	if config.RetryAfter <= 0 {
		config.RetryAfter = defaults.RetryAfter
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = defaults.MaxAttempts
	}
	return &Topic{
		Name:     name,
		commands: make(chan TopicCommand),
		config:   config,
		channels: make(map[uuid.UUID]*Channel),
		inFlight: make(map[uuid.UUID]int),
		unacked:  make(map[uuid.UUID]*DeliveryMessage),
		pending:  make([]*RawMessage, 0, config.PendingCapacity),
	}
}

func (t *Topic) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(t.config.RetryAfter)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			t.retryExpired(now)
		case command := <-t.commands:
			result := t.handle(command)
			if command.Reply != nil {
				command.Reply <- result
			}
		}
	}
}

func (t *Topic) handle(command TopicCommand) TopicResult {
	result := TopicResult{}
	switch command.Kind {
	case TopicSubscribe:
		if command.Channel.ID == uuid.Nil || command.Channel.Type != ConsumerChannel {
			result.Err = ErrInvalidChannel
			break
		}
		if command.Channel.OutBuffer == nil {
			command.Channel.OutBuffer = make(chan *DeliveryMessage, t.config.MaxInFlight)
		}
		id := command.Channel.ID
		if _, exists := t.channels[id]; !exists {
			channel := command.Channel
			t.channels[id] = &channel
			t.order = append(t.order, id)
			t.inFlight[id] = 0
		} else {
			channel := command.Channel
			t.channels[id] = &channel
		}
		t.drain()
	case TopicUnsubscribe:
		if command.ChannelID == uuid.Nil {
			result.Err = ErrInvalidChannel
			break
		}
		if _, exists := t.channels[command.ChannelID]; !exists {
			break
		}
		t.requeueChannel(command.ChannelID)
		delete(t.channels, command.ChannelID)
		delete(t.inFlight, command.ChannelID)
		for i, id := range t.order {
			if id == command.ChannelID {
				t.order = append(t.order[:i], t.order[i+1:]...)
				if t.rrIdx >= len(t.order) && len(t.order) > 0 {
					t.rrIdx %= len(t.order)
				}
				break
			}
		}
		t.drain()
	case TopicPublish:
		if command.Payload == nil {
			result.Err = ErrNilPayload
			break
		}
		// Free any available consumer capacity before applying the queue limit.
		t.drain()
		if len(t.pending) >= t.config.PendingCapacity {
			result.Err = ErrQueueFull
			break
		}
		payload := *command.Payload
		if payload.ID == uuid.Nil {
			payload.ID = uuid.New()
		}
		if payload.Topic == "" {
			payload.Topic = t.Name
		}
		t.pending = append(t.pending, &payload)
		t.published++
		t.drain()
	case TopicAck:
		result.Err = t.ack(command.ChannelID, command.DeliveryID)
		t.drain()
	case TopicNack:
		result.Err = t.nack(command.ChannelID, command.DeliveryID)
		t.drain()
	case TopicQueryStats:
		// Stats are populated below.
	default:
		result.Err = errors.New("core: unknown topic command")
	}
	result.Stats = t.stats()
	return result
}

func (t *Topic) stats() TopicStats {
	return TopicStats{
		Name:        t.Name,
		Subscribers: len(t.channels),
		Published:   t.published,
		Pending:     len(t.pending),
		Unacked:     len(t.unacked),
		Dropped:     t.dropped,
	}
}

func (t *Topic) drain() {
	for len(t.pending) > 0 {
		if !t.tryDeliver(t.pending[0]) {
			return
		}
		t.pending = t.pending[1:]
	}
}

func (t *Topic) tryDeliver(raw *RawMessage) bool {
	if len(t.order) == 0 {
		return false
	}
	for offset := 0; offset < len(t.order); offset++ {
		index := (t.rrIdx + offset) % len(t.order)
		id := t.order[index]
		channel := t.channels[id]
		if channel == nil || t.inFlight[id] >= t.config.MaxInFlight {
			continue
		}
		delivery := NewDeliveryMessage(raw, channel)
		delivery.Meta.Deadline = time.Now().Add(t.config.RetryAfter)
		select {
		case channel.OutBuffer <- delivery:
			t.unacked[delivery.Meta.DeliveryID] = delivery
			t.inFlight[id]++
			t.rrIdx = (index + 1) % len(t.order)
			return true
		default:
			// Try another consumer before applying backpressure to the topic.
		}
	}
	return false
}

func (t *Topic) ack(channelID, deliveryID uuid.UUID) error {
	if channelID == uuid.Nil || deliveryID == uuid.Nil {
		return ErrInvalidDelivery
	}
	delivery, ok := t.unacked[deliveryID]
	if !ok {
		return ErrDeliveryNotFound
	}
	if delivery.Meta.RecvChannelID != channelID {
		return ErrInvalidDelivery
	}
	delete(t.unacked, deliveryID)
	if t.inFlight[channelID] > 0 {
		t.inFlight[channelID]--
	}
	return nil
}

func (t *Topic) nack(channelID, deliveryID uuid.UUID) error {
	if channelID == uuid.Nil || deliveryID == uuid.Nil {
		return ErrInvalidDelivery
	}
	delivery, ok := t.unacked[deliveryID]
	if !ok {
		return ErrDeliveryNotFound
	}
	if delivery.Meta.RecvChannelID != channelID {
		return ErrInvalidDelivery
	}
	delete(t.unacked, deliveryID)
	if t.inFlight[channelID] > 0 {
		t.inFlight[channelID]--
	}
	if delivery.Meta.Attempt >= t.config.MaxAttempts {
		t.dropped++
		return nil
	}
	t.requeueDelivery(delivery)
	return nil
}

func (t *Topic) requeueChannel(channelID uuid.UUID) {
	for deliveryID, delivery := range t.unacked {
		if delivery.Meta.RecvChannelID != channelID {
			continue
		}
		delete(t.unacked, deliveryID)
		if t.inFlight[channelID] > 0 {
			t.inFlight[channelID]--
		}
		t.requeueDelivery(delivery)
	}
}

func (t *Topic) retryExpired(now time.Time) {
	for deliveryID, delivery := range t.unacked {
		if now.Before(delivery.Meta.Deadline) {
			continue
		}
		delete(t.unacked, deliveryID)
		if t.inFlight[delivery.Meta.RecvChannelID] > 0 {
			t.inFlight[delivery.Meta.RecvChannelID]--
		}
		if delivery.Meta.Attempt >= t.config.MaxAttempts {
			t.dropped++
			continue
		}
		t.requeueDelivery(delivery)
	}
	t.drain()
}

func (t *Topic) requeueDelivery(delivery *DeliveryMessage) {
	if len(t.pending) >= t.config.PendingCapacity {
		t.dropped++
		return
	}
	t.pending = append(t.pending, &RawMessage{
		ID:                 delivery.Meta.MessageID,
		Topic:              t.Name,
		Content:            string(delivery.Content),
		PublisherChannelID: delivery.Meta.SendChannelID,
		Attempt:            delivery.Meta.Attempt + 1,
	})
}

func (t *Topic) submit(ctx context.Context, command TopicCommand) (TopicResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if command.Reply == nil {
		command.Reply = make(chan TopicResult, 1)
	}
	select {
	case t.commands <- command:
	case <-ctx.Done():
		return TopicResult{}, ctx.Err()
	}
	select {
	case result := <-command.Reply:
		return result, result.Err
	case <-ctx.Done():
		return TopicResult{}, ctx.Err()
	}
}
