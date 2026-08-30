package core

import (
	"net"
	"sync"

	"github.com/google/uuid"
)

type Process struct {
	ID       uuid.UUID
	Conn     net.Conn
	Channels map[uuid.UUID]Channel

	mu sync.RWMutex
}

func NewProcess(conn net.Conn) *Process {
	return &Process{ID: uuid.New(), Conn: conn, Channels: make(map[uuid.UUID]Channel)}
}

func (p *Process) RegisterChannel(channel Channel) error {
	if p == nil || channel.ID == uuid.Nil {
		return ErrInvalidChannel
	}
	if channel.OutBuffer == nil && channel.Type == ConsumerChannel {
		channel.OutBuffer = make(chan *DeliveryMessage, 32)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Channels == nil {
		p.Channels = make(map[uuid.UUID]Channel)
	}
	p.Channels[channel.ID] = channel
	return nil
}

func (p *Process) LookupChannel(id uuid.UUID) (Channel, bool) {
	if p == nil {
		return Channel{}, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	channel, ok := p.Channels[id]
	return channel, ok
}

func (p *Process) RemoveChannel(id uuid.UUID) (Channel, bool) {
	if p == nil {
		return Channel{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	channel, ok := p.Channels[id]
	if ok {
		delete(p.Channels, id)
	}
	return channel, ok
}

func (p *Process) ChannelsSnapshot() []Channel {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	channels := make([]Channel, 0, len(p.Channels))
	for _, channel := range p.Channels {
		channels = append(channels, channel)
	}
	return channels
}
