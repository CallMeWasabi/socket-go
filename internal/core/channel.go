package core

import "github.com/google/uuid"

type Channel struct {
	ID         uuid.UUID
	ConsumerID uuid.UUID
	OutBuffer  chan *DeliveryMessage
	Type       uint8 // 0 = publisher, 1 = consumer

	p *Process
}

const (
	PublisherChannel uint8 = iota
	ConsumerChannel
)
