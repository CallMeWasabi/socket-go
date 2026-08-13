package core

import (
	"net"

	"github.com/google/uuid"
)

type Consumer struct {
	Conn net.Conn  // 16 - 64 byte
	ID   uuid.UUID // 16 byte

	message chan *Message // 8 byte
}

func NewConsumer(conn net.Conn, queueSize int) *Consumer {
	return &Consumer{
		Conn: conn,
		ID:   uuid.New(),

		message: make(chan *Message, queueSize),
	}
}

func (c *Consumer) WritePump() {
	for msg := range c.message {
		c.Conn.Write(msg.Content[:msg.ContentLength])
	}
}

func (c *Consumer) Write(b []byte) {
	msg := &Message{}
	copy(msg.Content[:], b)

	c.message <- msg
}
