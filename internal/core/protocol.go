package core

// align the struct from biggest size to reduce internal padding
type Payload struct {
	Payload       [1024]byte
	PayloadLength int
}

type Cmd struct {
}

type Message struct {
	Content       [1024]byte
	Topic         string
	ContentLength int
}
