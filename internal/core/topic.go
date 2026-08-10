package core

import "github.com/google/uuid"

type Topic struct {
	AddPartition    chan bool
	RemovePartition chan bool

	AddGroup    chan *ConsumerGroup
	RemoveGroup chan uuid.UUID

	Name string

	partitions []*Partition
}

const defaultQueueSize = 128

func NewTopic(name string, partitionSize int) *Topic {
	partitions := make([]*Partition, 0, partitionSize)
	for i := range partitionSize {
		partitions[i] = NewPartition(i, defaultQueueSize)
	}

	return &Topic{
		AddPartition:    make(chan bool),
		RemovePartition: make(chan bool),

		AddGroup:    make(chan *ConsumerGroup),
		RemoveGroup: make(chan uuid.UUID),

		Name: name,

		partitions: partitions,
	}
}

func (t *Topic) Run() {
	for {
		select {
		case _ = <-t.AddPartition:
			partition := NewPartition(len(t.partitions)+1, defaultQueueSize)

			t.partitions = append(t.partitions, partition)
		case _ = <-t.RemovePartition:
			t.partitions = t.partitions[:len(t.partitions)-1]
		}
	}
}
