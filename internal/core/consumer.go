package core

import (
	"net"

	"github.com/google/uuid"
)

type Consumer struct {
	Conn net.Conn  // 16 - 64 byte
	ID   uuid.UUID // 16 byte

	message chan *DeliveryMeta // 8 byte
}

func NewConsumer(conn net.Conn, queueSize int) *Consumer {
	return &Consumer{
		Conn: conn,
		ID:   uuid.New(),

		message: make(chan *DeliveryMeta, queueSize),
	}
}
