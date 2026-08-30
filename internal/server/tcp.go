package server

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/CallMeWasabi/socket-go/internal/protocol"
	"github.com/google/uuid"
)

type Handler interface {
	Handle(context.Context, *Session, protocol.FullFrame) error
}

type HandlerFunc func(context.Context, *Session, protocol.FullFrame) error

func (f HandlerFunc) Handle(ctx context.Context, session *Session, message protocol.FullFrame) error {
	return f(ctx, session, message)
}

type TCPServer struct {
	listener net.Listener
	handler  Handler
	onClose  func(*Session)

	closeOnce sync.Once
	wg        sync.WaitGroup
	sessions  sync.Map
}

func NewTCPServer(listener net.Listener, handler Handler) *TCPServer {
	if handler == nil {
		handler = HandlerFunc(func(context.Context, *Session, protocol.FullFrame) error { return nil })
	}
	return &TCPServer{listener: listener, handler: handler}
}

func Listen(addr string, handler Handler) (*TCPServer, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewTCPServer(listener, handler), nil
}

func (s *TCPServer) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *TCPServer) SetSessionCloseHandler(handler func(*Session)) {
	s.onClose = handler
}

func (s *TCPServer) Serve(ctx context.Context) error {
	if s.listener == nil {
		return errors.New("server: nil listener")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	stopCloser := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-stopCloser:
		}
	}()
	defer close(stopCloser)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			return err
		}

		session := newSession(conn)
		s.sessions.Store(session, struct{}{})
		s.wg.Add(1)
		go s.serveSession(ctx, session)
	}

	s.wg.Wait()
	return nil
}

func (s *TCPServer) serveSession(ctx context.Context, session *Session) {
	defer s.wg.Done()
	defer s.sessions.Delete(session)
	defer func() {
		if s.onClose != nil {
			s.onClose(session)
		}
	}()
	defer session.Close()

	processor := protocol.NewFrameProcessor()
	for {
		frame, err := protocol.ReadFrame(session.Conn)
		if err != nil {
			return
		}
		if err := processor.Record(frame); err != nil {
			return
		}

		message, err := processor.Build()
		if errors.Is(err, protocol.ErrIncompleteFrame) {
			continue
		}
		if err != nil {
			return
		}

		if err := s.handler.Handle(ctx, session, message); err != nil {
			return
		}
		processor.Reset()
	}
}

func (s *TCPServer) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.listener != nil {
			err = s.listener.Close()
		}
		s.sessions.Range(func(key, _ any) bool {
			_ = key.(*Session).Close()
			return true
		})
	})
	return err
}

type Session struct {
	Conn net.Conn
	ID   uuid.UUID

	writeMu   sync.Mutex
	closeOnce sync.Once
}

func newSession(conn net.Conn) *Session {
	return &Session{Conn: conn, ID: uuid.New()}
}

func (s *Session) SessionID() uuid.UUID {
	return s.ID
}

func (s *Session) SendFrame(frame *protocol.Frame) error {
	if frame == nil {
		return errors.New("server: nil frame")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := frame.WriteTo(s.Conn)
	return err
}

// SendMessage writes a logical message atomically with respect to other writers.
func (s *Session) SendMessage(frames ...protocol.Frame) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	for i := range frames {
		if _, err := frames[i].WriteTo(s.Conn); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.Conn != nil {
			err = s.Conn.Close()
		}
	})
	return err
}
