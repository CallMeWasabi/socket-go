package core

import (
	"fmt"

	"github.com/google/uuid"
)

// Broker add consumer, delete consumer, routing msg partition
type Broker struct {
<<<<<<< HEAD
	Subscribe   chan *SubscribeCmd
	Unsubscribe chan *UnsubscribeCmd
	Publish     chan *Message
=======
	addTopic    chan string
	removeTopic chan string
	subscribe   chan *ConsumerGroup
	unsubscribe chan uuid.UUID
>>>>>>> 558ba0f7335a6e2aef0b7a94b2f2033ec142da3d

	ID uuid.UUID

	topicManager *TopicManager
}

func NewBroker() *Broker {
	return &Broker{
<<<<<<< HEAD
		Subscribe:   make(chan *SubscribeCmd),
		Unsubscribe: make(chan *UnsubscribeCmd),
		Publish:     make(chan *Message),
=======
		addTopic:    make(chan string),
		removeTopic: make(chan string),
		subscribe:   make(chan *ConsumerGroup),
>>>>>>> 558ba0f7335a6e2aef0b7a94b2f2033ec142da3d

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
