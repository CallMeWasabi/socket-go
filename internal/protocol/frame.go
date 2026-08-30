package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// Wire format: type (1), channel (1), body length (4), body (N).
	FrameHeaderSize = 6
	MaxBodySize     = 1024
)

type FrameType uint8

const (
	HeaderType FrameType = iota
	MethodType
	BodyType
	HeartbeatType
	EndType
)

// Compatibility aliases for the initial draft names.
const (
	MethoType    = MethodType
	BodType      = BodyType
	HeartbeaType = HeartbeatType
)

var (
	ErrBodyTooLarge     = errors.New("protocol: frame body is too large")
	ErrInvalidFrameType = errors.New("protocol: invalid frame type")
	ErrInvalidFrame     = errors.New("protocol: invalid frame")
	ErrTrailingData     = errors.New("protocol: trailing data after frame")
)

type Frame struct {
	Type    FrameType
	Channel uint8
	Length  uint32
	Body    [MaxBodySize]byte
}

func NewFrame(t FrameType, channel uint8) *Frame {
	return &Frame{Type: t, Channel: channel}
}

func (f *Frame) SetBody(body []byte) error {
	if len(body) > MaxBodySize {
		return ErrBodyTooLarge
	}
	if err := validFrameType(f.Type); err != nil {
		return err
	}

	clear(f.Body[:])
	copy(f.Body[:], body)
	f.Length = uint32(len(body))
	return nil
}

func (f *Frame) WriteString(s string) error {
	return f.SetBody([]byte(s))
}

// Encode returns exactly one frame, without padding the body to MaxBodySize.
func (f *Frame) Encode() ([]byte, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}

	encoded := make([]byte, FrameHeaderSize+int(f.Length))
	encoded[0] = byte(f.Type)
	encoded[1] = f.Channel
	binary.BigEndian.PutUint32(encoded[2:FrameHeaderSize], f.Length)
	copy(encoded[FrameHeaderSize:], f.Body[:f.Length])
	return encoded, nil
}

// WriteTo writes one complete frame and handles short writes.
func (f *Frame) WriteTo(w io.Writer) (int64, error) {
	if err := f.validate(); err != nil {
		return 0, err
	}

	var header [FrameHeaderSize]byte
	header[0] = byte(f.Type)
	header[1] = f.Channel
	binary.BigEndian.PutUint32(header[2:], f.Length)
	headerBytes, err := writeFull(w, header[:])
	if err != nil {
		return int64(headerBytes), err
	}
	bodyBytes, err := writeFull(w, f.Body[:f.Length])
	return int64(headerBytes + bodyBytes), err
}

func ReadFrame(r io.Reader) (Frame, error) {
	var header [FrameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}

	frame := Frame{
		Type:    FrameType(header[0]),
		Channel: header[1],
		Length:  binary.BigEndian.Uint32(header[2:]),
	}
	if err := frame.validate(); err != nil {
		return Frame{}, err
	}
	if frame.Length == 0 {
		return frame, nil
	}
	if _, err := io.ReadFull(r, frame.Body[:frame.Length]); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func Decode(data []byte) (Frame, error) {
	if len(data) < FrameHeaderSize {
		return Frame{}, fmt.Errorf("%w: need at least %d bytes, got %d", ErrInvalidFrame, FrameHeaderSize, len(data))
	}

	frame, err := ReadFrame(bytes.NewReader(data))
	if err != nil {
		return Frame{}, err
	}
	wantSize := FrameHeaderSize + int(frame.Length)
	if len(data) != wantSize {
		return Frame{}, fmt.Errorf("%w: got %d bytes, want %d", ErrTrailingData, len(data), wantSize)
	}
	return frame, nil
}

func (f *Frame) validate() error {
	if err := validFrameType(f.Type); err != nil {
		return err
	}
	if f.Length > MaxBodySize {
		return ErrBodyTooLarge
	}
	return nil
}

func validFrameType(t FrameType) error {
	if t > EndType {
		return ErrInvalidFrameType
	}
	return nil
}

func writeFull(w io.Writer, data []byte) (int, error) {
	total := 0
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			total += n
			data = data[n:]
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

type FullFrame struct {
	Header  map[string]string
	Channel uint8
	Method  string
	Length  uint32
	Body    *bytes.Buffer
}

func NewFullFrame() *FullFrame {
	return &FullFrame{
		Header: make(map[string]string),
		Body:   new(bytes.Buffer),
	}
}
