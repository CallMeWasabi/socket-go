package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/CallMeWasabi/socket-go/internal/core"
)

var Exchange = core.NewExchange()

func main() {
	const port = 8080

	lc := net.ListenConfig{
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     30 * time.Second,
			Interval: 5 * time.Second,
			Count:    5,
		},
	}

	listener, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal("listening error:", err)
	}
	defer listener.Close()

	go Exchange.Run()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("error accepting conn:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
	}
}
