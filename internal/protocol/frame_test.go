package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestFrameEncodeDecode(t *testing.T) {
	want := NewFrame(MethodType, 7)
	if err := want.WriteString("PUBLISH"); err != nil {
		t.Fatal(err)
	}

	encoded, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}

	got, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if got.Type != want.Type || got.Channel != want.Channel || got.Length != want.Length {
		t.Fatalf("decoded header = %#v, want %#v", got, want)
	}
	if string(got.Body[:got.Length]) != "PUBLISH" {
		t.Fatalf("decoded body = %q, want %q", got.Body[:got.Length], "PUBLISH")
	}
}

func TestFrameRejectsOversizedBody(t *testing.T) {
	frame := NewFrame(BodyType, 1)
	if err := frame.SetBody(bytes.Repeat([]byte{'x'}, MaxBodySize+1)); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("SetBody() error = %v, want %v", err, ErrBodyTooLarge)
	}
}

func TestReadFrameHandlesPartialReads(t *testing.T) {
	want := NewFrame(BodyType, 2)
	if err := want.WriteString("payload"); err != nil {
		t.Fatal(err)
	}
	encoded, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}

	got, err := ReadFrame(&oneByteReader{data: encoded})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Body[:got.Length]) != "payload" {
		t.Fatalf("decoded body = %q, want payload", got.Body[:got.Length])
	}
}

func TestReadFrameRejectsOversizedLength(t *testing.T) {
	header := make([]byte, FrameHeaderSize)
	header[0] = byte(BodyType)
	binary.BigEndian.PutUint32(header[2:], MaxBodySize+1)

	_, err := ReadFrame(bytes.NewReader(header))
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("ReadFrame() error = %v, want %v", err, ErrBodyTooLarge)
	}
}

func TestReadFrameRejectsTruncatedBody(t *testing.T) {
	header := make([]byte, FrameHeaderSize+2)
	header[0] = byte(BodyType)
	binary.BigEndian.PutUint32(header[2:], 3)

	_, err := ReadFrame(bytes.NewReader(header))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadFrame() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestDecodeRejectsTrailingData(t *testing.T) {
	frame := frameWithBody(BodyType, 1, "x")
	encoded, err := frame.Encode()
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, 'y')

	_, err = Decode(encoded)
	if !errors.Is(err, ErrTrailingData) {
		t.Fatalf("Decode() error = %v, want %v", err, ErrTrailingData)
	}
}

func TestFrameWriteToHandlesShortWrites(t *testing.T) {
	frame := frameWithBody(BodyType, 4, "payload")
	writer := &shortWriter{max: 2}

	if _, err := frame.WriteTo(writer); err != nil {
		t.Fatal(err)
	}
	got, err := Decode(writer.data)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Body[:got.Length]) != "payload" {
		t.Fatalf("body = %q, want payload", got.Body[:got.Length])
	}
}

func TestFrameProcessorBuildsMessage(t *testing.T) {
	processor := NewFrameProcessor()
	frames := []Frame{
		*frameWithBody(HeaderType, 3, "content-type=text/plain\nrequest-id=42\n"),
		*frameWithBody(MethodType, 3, "PUBLISH"),
		*frameWithBody(BodyType, 3, "hello "),
		*frameWithBody(BodyType, 3, "world"),
		*NewFrame(EndType, 3),
	}

	for _, frame := range frames {
		if err := processor.Record(frame); err != nil {
			t.Fatalf("Record(%#v): %v", frame.Type, err)
		}
	}

	got, err := processor.Build()
	if err != nil {
		t.Fatal(err)
	}
	if got.Channel != 3 || got.Method != "PUBLISH" || got.Body.String() != "hello world" {
		t.Fatalf("built frame = %#v, want channel 3, PUBLISH, hello world", got)
	}
	if got.Header["request-id"] != "42" {
		t.Fatalf("headers = %#v, want request-id 42", got.Header)
	}
}

func TestFrameProcessorRejectsInvalidSequence(t *testing.T) {
	processor := NewFrameProcessor()
	if err := processor.Record(*frameWithBody(BodyType, 1, "body")); !errors.Is(err, ErrInvalidFrameSequence) {
		t.Fatalf("Record() error = %v, want %v", err, ErrInvalidFrameSequence)
	}
	if err := processor.Record(*frameWithBody(HeaderType, 1, "request-id=1")); err != nil {
		t.Fatalf("processor state changed after rejected frame: %v", err)
	}
}

func TestFrameProcessorRejectsChannelMismatch(t *testing.T) {
	processor := NewFrameProcessor()
	if err := processor.Record(*frameWithBody(HeaderType, 1, "request-id=1\n")); err != nil {
		t.Fatal(err)
	}
	if err := processor.Record(*frameWithBody(MethodType, 2, "PUBLISH")); !errors.Is(err, ErrChannelMismatch) {
		t.Fatalf("Record() error = %v, want %v", err, ErrChannelMismatch)
	}
}

func TestFrameProcessorRejectsMalformedHeaderOnBuild(t *testing.T) {
	processor := NewFrameProcessor()
	for _, frame := range []Frame{
		*frameWithBody(HeaderType, 1, "not-a-header"),
		*frameWithBody(MethodType, 1, "PUBLISH"),
		*NewFrame(EndType, 1),
	} {
		if err := processor.Record(frame); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := processor.Build(); !errors.Is(err, ErrMalformedHeader) {
		t.Fatalf("Build() error = %v, want %v", err, ErrMalformedHeader)
	}
}

func TestFrameProcessorReset(t *testing.T) {
	processor := NewFrameProcessor()
	for _, frame := range []Frame{
		*frameWithBody(HeaderType, 1, "request-id=1"),
		*frameWithBody(MethodType, 1, "PING"),
		*NewFrame(EndType, 1),
	} {
		if err := processor.Record(frame); err != nil {
			t.Fatal(err)
		}
	}
	processor.Reset()
	if processor.Offset != 0 || len(processor.Frames) != 0 {
		t.Fatalf("processor was not reset: %#v", processor)
	}
	if err := processor.Record(*frameWithBody(HeaderType, 2, "request-id=2")); err != nil {
		t.Fatal(err)
	}
}

func TestFrameProcessorRejectsOversizedMessage(t *testing.T) {
	processor := NewFrameProcessor()
	if err := processor.Record(*frameWithBody(HeaderType, 1, "request-id=1")); err != nil {
		t.Fatal(err)
	}
	if err := processor.Record(*frameWithBody(MethodType, 1, "PUBLISH")); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < MaxMessageSize/MaxBodySize; i++ {
		if err := processor.Record(*frameWithBytes(BodyType, 1, bytes.Repeat([]byte{'x'}, MaxBodySize))); err != nil {
			t.Fatal(err)
		}
	}
	if err := processor.Record(*frameWithBody(BodyType, 1, "x")); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Record() error = %v, want %v", err, ErrMessageTooLarge)
	}
}

func frameWithBody(t FrameType, channel uint8, body string) *Frame {
	return frameWithBytes(t, channel, []byte(body))
}

func frameWithBytes(t FrameType, channel uint8, body []byte) *Frame {
	frame := NewFrame(t, channel)
	if err := frame.SetBody(body); err != nil {
		panic(err)
	}
	return frame
}

type oneByteReader struct {
	data []byte
}

type shortWriter struct {
	data []byte
	max  int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.max {
		p = p[:w.max]
	}
	w.data = append(w.data, p...)
	return len(p), nil
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}
