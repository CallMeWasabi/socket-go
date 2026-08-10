package core

import (
	"github.com/CallMeWasabi/socket-go/internal/collection"
	"github.com/google/uuid"
)

type PartitionKey struct {
	Topic       string
	PartitionID int
}

type ConsumerGroup struct {
	ID          uuid.UUID
	Assignments map[PartitionKey]uuid.UUID
	Consumers   map[uuid.UUID]*Consumer
}

func NewGroup() *ConsumerGroup {
	return &ConsumerGroup{
		ID:          uuid.New(),
		Assignments: make(map[PartitionKey]uuid.UUID),
		Consumers:   make(map[uuid.UUID]*Consumer),
	}
}

// balance partition to consumer
func (cg *ConsumerGroup) balanced() {
	topic := map[string]
}

func (cg *ConsumerGroup) Add(c *Consumer) {
	if _, ok := cg.Consumers[c.ID]; ok {
		return
	}

	cg.Consumers[c.ID] = c
	cg.balanced()
}

func (cg *ConsumerGroup) Remove(connID uuid.UUID) {
	if _, ok := cg.Consumers[connID]; !ok {
		return
	}

	delete(cg.Consumers, connID)
	cg.balanced()
}
