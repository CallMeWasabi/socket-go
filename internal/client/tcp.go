package client

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/CallMeWasabi/socket-go/internal/protocol"
)

type Client struct {
	Conn net.Conn

	writeMu sync.Mutex
	readMu  sync.Mutex
	parser  protocol.FrameProcessor
}

func Dial(ctx context.Context, addr string) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return New(conn), nil
}

func New(conn net.Conn) *Client {
	return &Client{Conn: conn, parser: protocol.NewFrameProcessor()}
}

func (c *Client) SendFrame(frame *protocol.Frame) error {
	if frame == nil {
		return errors.New("client: nil frame")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := frame.WriteTo(c.Conn)
	return err
}

// SendMessage writes all frames in order while holding the connection write lock.
func (c *Client) SendMessage(frames ...protocol.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for i := range frames {
		if _, err := frames[i].WriteTo(c.Conn); err != nil {
			return err
		}
	}
	return nil
}

// ReadMessage reads frames until the processor sees End or a standalone heartbeat.
// Only one goroutine should call ReadMessage at a time.
func (c *Client) ReadMessage() (protocol.FullFrame, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		frame, err := protocol.ReadFrame(c.Conn)
		if err != nil {
			return protocol.FullFrame{}, err
		}
		if err := c.parser.Record(frame); err != nil {
			c.parser.Reset()
			return protocol.FullFrame{}, err
		}

		message, err := c.parser.Build()
		if errors.Is(err, protocol.ErrIncompleteFrame) {
			continue
		}
		if err != nil {
			c.parser.Reset()
			return protocol.FullFrame{}, err
		}
		c.parser.Reset()
		return message, nil
	}
}

func (c *Client) Close() error {
	if c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}
