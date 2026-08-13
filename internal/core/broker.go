package core

import (
	"fmt"

	"github.com/google/uuid"
)

// Broker add consumer, delete consumer, routing msg partition
type Broker struct {
	Subscribe   chan *SubscribeCmd
	Unsubscribe chan *UnsubscribeCmd
	Publish     chan *Message

	ID uuid.UUID

	topicManager *TopicManager
}

func NewBroker() *Broker {
	return &Broker{
		Subscribe:   make(chan *SubscribeCmd),
		Unsubscribe: make(chan *UnsubscribeCmd),
		Publish:     make(chan *Message),

		ID: uuid.New(),

		topicManager: newTopicManager(),
	}
}

func (b *Broker) Run() {
	for {
		select {
		case cmd := <-b.Subscribe:
			b.topicManager.assignConsumer(cmd)
		case cmd := <-b.Unsubscribe:
			b.topicManager.unassignConsumer(cmd)
		case msg := <-b.Publish:
			b.topicManager.publish(msg)
		}
	}
}

func (b *Broker) Topics() []byte {
	reports := []byte{}
	for k, v := range b.topicManager.topics {
		reports = fmt.Appendf(reports, "%s %v\n", k, v)
	}

	return reports
}
