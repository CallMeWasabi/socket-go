package core

import "github.com/google/uuid"

type Partition struct {
	Messages chan *Message

	ID int

	groups map[uuid.UUID]*ConsumerGroup
}

func NewPartition(id int, queueSize int) *Partition {
	return &Partition{
		Messages: make(chan *Message, queueSize),
		ID:       id,
		groups:   make(map[uuid.UUID]*ConsumerGroup),
	}
}

func (p *Partition) Run() {
	for msg := range p.Messages {
		for _, v := range p.groups {
			key := PartitionKey{Topic: msg.Topic, PartitionID: p.ID}
			consumerID := v.Assignments[key]
			v.Consumers[consumerID].Messages <- msg
		}
	}
}
