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

var Broker = core.NewBroker()

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

	go Broker.Run()

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

	consumer := core.NewConsumer(conn, 128)

	buffSize := 4 * 1024
<<<<<<< HEAD
=======
	defaultQueueSize := 128

	consumer := core.NewConsumer(conn, defaultQueueSize)
>>>>>>> 558ba0f7335a6e2aef0b7a94b2f2033ec142da3d
	reader := bufio.NewReaderSize(consumer.Conn, buffSize)

	go consumer.WritePump()

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("read error: %v", err)
			break
		}

		cmd := strings.Split(strings.TrimSpace(msg), " ")

		fmt.Printf("recieve cmd: %v, len: %d\n", cmd, len(cmd))

		if len(cmd) == 1 {
			switch cmd[0] {
			case "topics":
				consumer.Conn.Write(Broker.Topics())
			}
		} else if len(cmd) == 2 {
			switch cmd[0] {
			case "sub":
				name := cmd[1]
				Broker.Subscribe <- &core.SubscribeCmd{
					Topic: name,
					C:     consumer,
				}

				consumer.Conn.Write(fmt.Appendf([]byte(""), "Subscribe topic %s success\n", name))
			case "unsub":
				name := cmd[1]
				Broker.Unsubscribe <- &core.UnsubscribeCmd{
					Topic: name,
					C:     consumer,
				}

				consumer.Conn.Write(fmt.Appendf([]byte(""), "Unsubscribe topic %s success\n", name))
			}
		} else if len(cmd) == 3 {
			switch cmd[0] {
			case "publish":
				contentLength := len(cmd[2])
				msg := &core.Message{
					Topic:         cmd[1],
					ContentLength: contentLength,
				}
				copy(msg.Content[:], cmd[2])

				Broker.Publish <- msg
			}
		}
	}

	consumer.Close()
}
