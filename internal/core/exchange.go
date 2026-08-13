package core

import (
	"github.com/google/uuid"
)

// Exchange add consumer, delete consumer, routing msg partition
type Exchange struct {
	ID uuid.UUID
}

func NewExchange() *Exchange {
	return &Exchange{
		ID: uuid.New(),
	}
}

func (b *Exchange) Run() {
	for {
		select {}
	}
}
