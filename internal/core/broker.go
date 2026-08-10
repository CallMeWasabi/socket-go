package core

import (
	"github.com/google/uuid"
)

type Broker struct {
	addTopic    chan string
	removeTopic chan string
	subscribe   chan *ConsumerGroup
	unsubscribe chan uuid.UUID

	ID uuid.UUID

	topics map[string]*Topic
}

const defaultPartitionSize = 3

func NewBroker() *Broker {
	return &Broker{
		addTopic:    make(chan string),
		removeTopic: make(chan string),
		subscribe:   make(chan *ConsumerGroup),

		ID: uuid.New(),

		topics: make(map[string]*Topic),
	}
}

func (b *Broker) Run() {
	for {
		select {
		case name := <-b.addTopic:
			if _, ok := b.topics[name]; ok {
				continue
			}

			b.topics[name] = NewTopic(name, defaultPartitionSize)
		case id := <-b.removeTopic:
			if _, ok := b.topics[id]; !ok {
				continue
			}

			delete(b.topics, id)
		}
	}
}
