package core

import (
	"net"
	"slices"

	"github.com/google/uuid"
)

type Consumer struct {
	ID     uuid.UUID // 16 byte
	Conn   net.Conn  // 16 byte
	Topics []string
}

// sub topic
func (c *Consumer) Subscribe(z *Zookeeper, topic string) error {
	if slices.Contains(c.Topics, topic) {
		return nil
	}

	c.Topics = append(c.Topics, topic)
	z.AddConsumer(c, topic)

	return nil
}
