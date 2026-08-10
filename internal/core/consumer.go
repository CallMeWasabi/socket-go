package core

import (
	"net"

	"github.com/google/uuid"
)

type Consumer struct {
	Messages chan *Message

	ID   uuid.UUID // 16 byte
	Conn net.Conn  // 16 byte
}

func NewConsumer(conn net.Conn, queueSize int) *Consumer {
	return &Consumer{
		Messages: make(chan *Message, queueSize),
		ID:       uuid.New(),
		Conn:     conn,
	}
}
