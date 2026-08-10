package core

import "github.com/google/uuid"

type PartitionKey struct {
	Topic       string
	PartitionID int
}

type ConsumerGroup struct {
	ID uuid.UUID

	Assignments map[PartitionKey]uuid.UUID
	Consumers   map[uuid.UUID]*Consumer
}
