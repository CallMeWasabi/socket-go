package core

import "github.com/google/uuid"

type Payload struct {
	Message string
}

type MesssageMeta struct {
	MessageID       uuid.UUID
	ConsumerID      uuid.UUID
	AssignAt        uint64
	TimeoutAt       uint64
	PayloadPointer  *Payload
	RedeliveryCount uint32
}

type Queue struct {
	ID             uuid.UUID
	UnackedMessage map[uuid.UUID]MesssageMeta
	Channels       []Channel
}
