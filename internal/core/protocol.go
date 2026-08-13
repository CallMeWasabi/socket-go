package core

// Payload align the struct from biggest size to reduce internal padding
type Payload struct {
	Payload       [1024]byte
	PayloadLength int
}

<<<<<<< HEAD
type SubscribeCmd struct {
	Topic string
	C     *Consumer
}

type UnsubscribeCmd struct {
	Topic string
	C     *Consumer
}
=======
type Cmd struct{}
>>>>>>> 558ba0f7335a6e2aef0b7a94b2f2033ec142da3d

type Message struct {
	Content       [1024]byte
	Topic         string
	ContentLength int
}
