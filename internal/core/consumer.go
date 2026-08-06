package core

import (
	"net"

	"github.com/google/uuid"
)

type Consumer struct {
	ID     uuid.UUID
	Conn   net.Conn
	Topics []string
}

// sub topic
func (c *Consumer) Subscribe(title string) {
}
