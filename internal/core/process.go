package core

import (
	"net"

	"github.com/google/uuid"
)

// we plug frame processor to process

type Process struct {
	ID       uuid.UUID
	Conn     net.Conn
	Channels []Channel
}
