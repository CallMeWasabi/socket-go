package core

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestExchangeDeliversMessageAndAckRemovesUnacked(t *testing.T) {
	exchange := NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exchange.Run(ctx)

	consumerChannel := Channel{
		ID:         uuid.New(),
		ConsumerID: uuid.New(),
		Type:       ConsumerChannel,
		OutBuffer:  make(chan *DeliveryMessage, 1),
	}
	if err := exchange.Subscribe(ctx, "orders", consumerChannel); err != nil {
		t.Fatal(err)
	}

	publisherChannelID := uuid.New()
	if err := exchange.PublishFrom(ctx, "orders", publisherChannelID, &RawMessage{Content: "hello"}); err != nil {
		t.Fatal(err)
	}

	var delivery *DeliveryMessage
	select {
	case delivery = <-consumerChannel.OutBuffer:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivery")
	}
	if delivery == nil {
		t.Fatal("received nil delivery")
	}
	if string(delivery.Content) != "hello" {
		t.Fatalf("content = %q, want hello", delivery.Content)
	}
	if delivery.Meta.SendChannelID != publisherChannelID {
		t.Fatalf("send channel = %v, want %v", delivery.Meta.SendChannelID, publisherChannelID)
	}
	if delivery.Meta.RecvChannelID != consumerChannel.ID {
		t.Fatalf("receive channel = %v, want %v", delivery.Meta.RecvChannelID, consumerChannel.ID)
	}

	stats, err := exchange.Stats(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Unacked != 1 {
		t.Fatalf("unacked = %d, want 1", stats.Unacked)
	}

	if err := exchange.Ack(ctx, "orders", consumerChannel.ID, delivery.Meta.DeliveryID); err != nil {
		t.Fatal(err)
	}
	stats, err = exchange.Stats(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Unacked != 0 {
		t.Fatalf("unacked after ack = %d, want 0", stats.Unacked)
	}
}

func TestExchangeQueuesUntilConsumerHasCapacity(t *testing.T) {
	exchange := NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exchange.Run(ctx)

	consumerChannel := Channel{
		ID:         uuid.New(),
		ConsumerID: uuid.New(),
		Type:       ConsumerChannel,
		OutBuffer:  make(chan *DeliveryMessage, 1),
	}
	if err := exchange.Subscribe(ctx, "orders", consumerChannel); err != nil {
		t.Fatal(err)
	}

	for _, body := range []string{"one", "two"} {
		if err := exchange.PublishFrom(ctx, "orders", uuid.New(), &RawMessage{Content: body}); err != nil {
			t.Fatal(err)
		}
	}

	first := <-consumerChannel.OutBuffer
	if string(first.Content) != "one" {
		t.Fatalf("first content = %q, want one", first.Content)
	}
	// The second message remains pending because the channel has not ACKed
	// the first delivery yet.
	stats, err := exchange.Stats(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1 || stats.Unacked != 1 {
		t.Fatalf("stats = %#v, want pending=1/unacked=1", stats)
	}

	if err := exchange.Ack(ctx, "orders", consumerChannel.ID, first.Meta.DeliveryID); err != nil {
		t.Fatal(err)
	}
	select {
	case second := <-consumerChannel.OutBuffer:
		if string(second.Content) != "two" {
			t.Fatalf("second content = %q, want two", second.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second delivery")
	}
}

func TestTopicRetriesUnackedDelivery(t *testing.T) {
	topic := NewTopicWithConfig("orders", TopicConfig{
		PendingCapacity: 4,
		MaxInFlight:     1,
		RetryAfter:      10 * time.Millisecond,
		MaxAttempts:     2,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topic.Run(ctx)

	channel := Channel{
		ID:         uuid.New(),
		ConsumerID: uuid.New(),
		Type:       ConsumerChannel,
		OutBuffer:  make(chan *DeliveryMessage, 1),
	}
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicSubscribe, Channel: channel}); err != nil {
		t.Fatal(err)
	}
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: &RawMessage{Content: "retry"}}); err != nil {
		t.Fatal(err)
	}

	first := <-channel.OutBuffer
	select {
	case second := <-channel.OutBuffer:
		if second.Meta.MessageID != first.Meta.MessageID {
			t.Fatalf("retry message id = %v, want %v", second.Meta.MessageID, first.Meta.MessageID)
		}
		if second.Meta.Attempt != 2 {
			t.Fatalf("retry attempt = %d, want 2", second.Meta.Attempt)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for retry")
	}
}

func TestTopicRoundRobinAcrossConsumers(t *testing.T) {
	topic := NewTopicWithConfig("orders", TopicConfig{PendingCapacity: 8, MaxInFlight: 1, RetryAfter: time.Second, MaxAttempts: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topic.Run(ctx)

	firstChannel := Channel{ID: uuid.New(), ConsumerID: uuid.New(), Type: ConsumerChannel, OutBuffer: make(chan *DeliveryMessage, 1)}
	secondChannel := Channel{ID: uuid.New(), ConsumerID: uuid.New(), Type: ConsumerChannel, OutBuffer: make(chan *DeliveryMessage, 1)}
	for _, channel := range []Channel{firstChannel, secondChannel} {
		if _, err := topic.submit(ctx, TopicCommand{Kind: TopicSubscribe, Channel: channel}); err != nil {
			t.Fatal(err)
		}
	}
	for _, body := range []string{"one", "two"} {
		if _, err := topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: &RawMessage{Content: body}}); err != nil {
			t.Fatal(err)
		}
	}

	first := <-firstChannel.OutBuffer
	second := <-secondChannel.OutBuffer
	if string(first.Content) != "one" || string(second.Content) != "two" {
		t.Fatalf("deliveries = %q/%q, want one/two", first.Content, second.Content)
	}
	if err := topic.submitAck(ctx, firstChannel.ID, first.Meta.DeliveryID); err != nil {
		t.Fatal(err)
	}
	if err := topic.submitAck(ctx, secondChannel.ID, second.Meta.DeliveryID); err != nil {
		t.Fatal(err)
	}
}

func (t *Topic) submitAck(ctx context.Context, channelID, deliveryID uuid.UUID) error {
	_, err := t.submit(ctx, TopicCommand{Kind: TopicAck, ChannelID: channelID, DeliveryID: deliveryID})
	return err
}

func TestTopicRejectsInvalidAck(t *testing.T) {
	topic := NewTopicWithConfig("orders", TopicConfig{PendingCapacity: 4, MaxInFlight: 1, RetryAfter: time.Second, MaxAttempts: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topic.Run(ctx)

	owner := Channel{ID: uuid.New(), ConsumerID: uuid.New(), Type: ConsumerChannel, OutBuffer: make(chan *DeliveryMessage, 1)}
	other := Channel{ID: uuid.New(), ConsumerID: uuid.New(), Type: ConsumerChannel, OutBuffer: make(chan *DeliveryMessage, 1)}
	for _, channel := range []Channel{owner, other} {
		if _, err := topic.submit(ctx, TopicCommand{Kind: TopicSubscribe, Channel: channel}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: &RawMessage{Content: "one"}}); err != nil {
		t.Fatal(err)
	}
	delivery := <-owner.OutBuffer
	if err := topic.submitAck(ctx, other.ID, delivery.Meta.DeliveryID); err != ErrInvalidDelivery {
		t.Fatalf("invalid ack error = %v, want %v", err, ErrInvalidDelivery)
	}
	if err := topic.submitAck(ctx, owner.ID, delivery.Meta.DeliveryID); err != nil {
		t.Fatal(err)
	}
}

func TestTopicQueueCapacity(t *testing.T) {
	topic := NewTopicWithConfig("orders", TopicConfig{PendingCapacity: 1, MaxInFlight: 1, RetryAfter: time.Second, MaxAttempts: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topic.Run(ctx)

	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: &RawMessage{Content: "one"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: &RawMessage{Content: "two"}}); err != ErrQueueFull {
		t.Fatalf("second publish error = %v, want %v", err, ErrQueueFull)
	}
}

func TestTopicDeliversMessagesPublishedBeforeSubscribe(t *testing.T) {
	topic := NewTopicWithConfig("orders", TopicConfig{PendingCapacity: 4, MaxInFlight: 1, RetryAfter: time.Second, MaxAttempts: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topic.Run(ctx)

	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: &RawMessage{Content: "queued"}}); err != nil {
		t.Fatal(err)
	}
	channel := Channel{ID: uuid.New(), ConsumerID: uuid.New(), Type: ConsumerChannel, OutBuffer: make(chan *DeliveryMessage, 1)}
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicSubscribe, Channel: channel}); err != nil {
		t.Fatal(err)
	}
	select {
	case delivery := <-channel.OutBuffer:
		if string(delivery.Content) != "queued" {
			t.Fatalf("content = %q, want queued", delivery.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued delivery")
	}
}

func TestTopicUnsubscribeRequeuesDelivery(t *testing.T) {
	topic := NewTopicWithConfig("orders", TopicConfig{PendingCapacity: 8, MaxInFlight: 1, RetryAfter: time.Second, MaxAttempts: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topic.Run(ctx)

	first := Channel{ID: uuid.New(), ConsumerID: uuid.New(), Type: ConsumerChannel, OutBuffer: make(chan *DeliveryMessage, 1)}
	second := Channel{ID: uuid.New(), ConsumerID: uuid.New(), Type: ConsumerChannel, OutBuffer: make(chan *DeliveryMessage, 1)}
	for _, channel := range []Channel{first, second} {
		if _, err := topic.submit(ctx, TopicCommand{Kind: TopicSubscribe, Channel: channel}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: &RawMessage{Content: "one"}}); err != nil {
		t.Fatal(err)
	}
	firstDelivery := <-first.OutBuffer
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicUnsubscribe, ChannelID: first.ID}); err != nil {
		t.Fatal(err)
	}
	select {
	case secondDelivery := <-second.OutBuffer:
		if secondDelivery.Meta.MessageID != firstDelivery.Meta.MessageID {
			t.Fatalf("requeued message id = %v, want %v", secondDelivery.Meta.MessageID, firstDelivery.Meta.MessageID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for requeued delivery")
	}
}

func TestTopicNackRequeuesDeliveryImmediately(t *testing.T) {
	topic := NewTopicWithConfig("orders", TopicConfig{PendingCapacity: 8, MaxInFlight: 1, RetryAfter: time.Hour, MaxAttempts: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topic.Run(ctx)

	channel := Channel{ID: uuid.New(), ConsumerID: uuid.New(), Type: ConsumerChannel, OutBuffer: make(chan *DeliveryMessage, 1)}
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicSubscribe, Channel: channel}); err != nil {
		t.Fatal(err)
	}
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicPublish, Payload: &RawMessage{Content: "one"}}); err != nil {
		t.Fatal(err)
	}
	first := <-channel.OutBuffer
	if _, err := topic.submit(ctx, TopicCommand{Kind: TopicNack, ChannelID: channel.ID, DeliveryID: first.Meta.DeliveryID}); err != nil {
		t.Fatal(err)
	}
	select {
	case second := <-channel.OutBuffer:
		if second.Meta.MessageID != first.Meta.MessageID || second.Meta.Attempt != 2 {
			t.Fatalf("nack delivery = %#v, want same message attempt 2", second.Meta)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nacked delivery")
	}
}
