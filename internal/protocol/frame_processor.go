package protocol

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidFrameSequence = errors.New("protocol: invalid frame sequence")
	ErrChannelMismatch      = errors.New("protocol: frame channel mismatch")
	ErrIncompleteFrame      = errors.New("protocol: frame sequence is incomplete")
	ErrMalformedHeader      = errors.New("protocol: malformed header")
	ErrProcessorComplete    = errors.New("protocol: processor already completed")
	ErrMessageTooLarge      = errors.New("protocol: logical message is too large")
)

const MaxMessageSize = 64 * 1024

type processorStage uint8

const (
	stageStart processorStage = iota
	stageMethod
	stageBody
	stageComplete
)

// FrameProcessor validates and assembles one logical message.
// A message is Header, Method, zero or more Body frames, then End.
type FrameProcessor struct {
	Offset uint8
	Frames []Frame

	channel uint8
	hasChan bool
	stage   processorStage
	bodyLen uint32
}

func NewFrameProcessor() FrameProcessor {
	return FrameProcessor{}
}

func (fp *FrameProcessor) Record(f Frame) error {
	if err := f.validate(); err != nil {
		return err
	}
	if fp.stage == stageComplete {
		return ErrProcessorComplete
	}
	if fp.hasChan && f.Channel != fp.channel {
		return ErrChannelMismatch
	}

	switch {
	case fp.stage == stageStart && f.Type == HeaderType:
		fp.stage = stageMethod
	case fp.stage == stageMethod && f.Type == MethodType:
		fp.stage = stageBody
	case fp.stage == stageBody && f.Type == BodyType:
		if fp.bodyLen+f.Length > MaxMessageSize {
			return ErrMessageTooLarge
		}
		fp.bodyLen += f.Length
		// Body frames can repeat.
	case fp.stage == stageBody && f.Type == EndType:
		fp.stage = stageComplete
	case fp.stage == stageStart && f.Type == HeartbeatType:
		fp.stage = stageComplete
	default:
		return ErrInvalidFrameSequence
	}
	if !fp.hasChan {
		fp.channel = f.Channel
		fp.hasChan = true
	}

	fp.Frames = append(fp.Frames, f)
	if fp.Offset < ^uint8(0) {
		fp.Offset++
	}
	return nil
}

func (fp *FrameProcessor) Reset() {
	fp.Offset = 0
	fp.Frames = fp.Frames[:0]
	fp.channel = 0
	fp.hasChan = false
	fp.stage = stageStart
	fp.bodyLen = 0
}

func (fp *FrameProcessor) Build() (FullFrame, error) {
	if fp.stage != stageComplete {
		return FullFrame{}, ErrIncompleteFrame
	}

	full := NewFullFrame()
	full.Channel = fp.channel
	for _, frame := range fp.Frames {
		switch frame.Type {
		case HeaderType:
			headers, err := parseHeaders(frame.Body[:frame.Length])
			if err != nil {
				return FullFrame{}, err
			}
			for key, value := range headers {
				full.Header[key] = value
			}
		case MethodType:
			full.Method = string(frame.Body[:frame.Length])
		case BodyType:
			_, _ = full.Body.Write(frame.Body[:frame.Length])
		}
	}
	full.Length = uint32(full.Body.Len())
	return *full, nil
}

// Header frame bodies use one key=value pair per line. Empty lines are ignored.
func parseHeaders(data []byte) (map[string]string, error) {
	headers := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("%w: %q", ErrMalformedHeader, line)
		}
		headers[key] = value
	}
	return headers, nil
}
