package server

import "net"

type TCPServer struct {
	listener net.Listener
	port     int
}

func (tcp *TCPServer) Serve() {
}

func (tcp *TCPServer) Close() {
	tcp.listener.Close()
}
