package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/CallMeWasabi/socket-go/internal/core"
)

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

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("error accepting conn:", err)
			continue
		}

		go handleConnection(conn)
	}
}

var Broker = core.NewBroker()

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buffSize := 4 * 1024
	defaultQueueSize := 128

	consumer := core.NewConsumer(conn, defaultQueueSize)
	reader := bufio.NewReaderSize(consumer.Conn, buffSize)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("read error: %v", err)
			break
		}

		cmd := strings.Split(msg, " ")

		if len(cmd) == 2 {
			switch cmd[0] {
			case "sub":
			case "unsub":
			}
		}
	}
}
