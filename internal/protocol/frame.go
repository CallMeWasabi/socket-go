package protocol

import (
	"bytes"
	"encoding/binary"
)

// frame structure of protocol

type FrameType uint8

const (
	HeaderType FrameType = iota
	MethoType
	BodType
	HeartbeaType
	EndType
)

type Frame struct {
	Type    uint8  // payload type = method, header, body, heartbeat, end
	Channel uint8  // channel id
	Length  uint32 // length of body
	Body    [1024]byte
}

func NewFrame(t uint8, channel uint8) *Frame {
	return &Frame{
		Type:    t,
		Channel: channel,
	}
}

func (f *Frame) WriteString(s string) {
	f.Length = uint32(len(s))
	copy(f.Body[:], s)
}

func (f *Frame) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, *f); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type FullFrame struct {
	Header  map[string]string
	Channel uint8
	Length  uint32
	Body    *bytes.Buffer
}

func NewFullFrame() *FullFrame {
	return &FullFrame{
		Header: make(map[string]string),
		Body:   new(bytes.Buffer),
	}
}
