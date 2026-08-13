package core

// Payload align the struct from biggest size to reduce internal padding
type Payload struct {
	Payload       [1024]byte
	PayloadLength int
}

type SubscribeCmd struct {
	Topic string
	C     *Consumer
}

type UnsubscribeCmd struct {
	Topic string
	C     *Consumer
}

type Message struct {
	Content       [1024]byte
	Topic         string
	ContentLength int
}
