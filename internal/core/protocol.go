package core

// align the struct from biggest size to reduce internal padding
type ProtocolMsg struct {
	Payload       [1024]byte
	CMD           [8]byte
	PayloadLength int
}
