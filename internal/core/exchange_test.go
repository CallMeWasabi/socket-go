package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestExchangeOwnsTopicRegistry(t *testing.T) {
	exchange := NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- exchange.Run(ctx) }()

	if err := exchange.AddTopic(context.Background(), "orders"); err != nil {
		t.Fatal(err)
	}
	if err := exchange.AddTopic(context.Background(), "users"); err != nil {
		t.Fatal(err)
	}
	topics, err := exchange.Topics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 2 || topics[0] != "orders" || topics[1] != "users" {
		t.Fatalf("topics = %#v, want sorted orders/users", topics)
	}

	if err := exchange.RemoveTopic(context.Background(), "users"); err != nil {
		t.Fatal(err)
	}
	topics, err = exchange.Topics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0] != "orders" {
		t.Fatalf("topics after remove = %#v, want orders", topics)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("exchange did not stop")
	}
}

func TestExchangeForwardsCommandsToTopicActor(t *testing.T) {
	exchange := NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exchange.Run(ctx)

	channel := Channel{ID: uuid.New(), Type: ConsumerChannel}
	if err := exchange.Subscribe(context.Background(), "orders", channel); err != nil {
		t.Fatal(err)
	}
	if err := exchange.Publish(context.Background(), "orders", &RawMessage{Content: "hello"}); err != nil {
		t.Fatal(err)
	}

	stats, err := exchange.Stats(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Subscribers != 1 || stats.Published != 1 {
		t.Fatalf("topic stats = %#v, want one subscriber and one publish", stats)
	}

	if err := exchange.Unsubscribe(context.Background(), "orders", channel.ID); err != nil {
		t.Fatal(err)
	}
	stats, err = exchange.Stats(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Subscribers != 0 {
		t.Fatalf("topic stats after unsubscribe = %#v, want zero subscribers", stats)
	}
}

func TestExchangeRejectsUnknownTopic(t *testing.T) {
	exchange := NewExchange()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go exchange.Run(ctx)

	err := exchange.Publish(ctx, "missing", &RawMessage{Content: "hello"})
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestExchangeRejectsInvalidTopicName(t *testing.T) {
	exchange := NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exchange.Run(ctx)

	if err := exchange.AddTopic(context.Background(), ""); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("AddTopic() error = %v, want %v", err, ErrInvalidTopicName)
	}
}
