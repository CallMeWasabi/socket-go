package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/CallMeWasabi/socket-go/internal/client"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "server address")
	flag.Parse()

	conn, err := client.Dial(context.Background(), *addr)
	if err != nil {
		log.Fatal("dial failed:", err)
	}
	defer conn.Close()

	go handleReader(conn, os.Stdout)
	if err := runCLI(conn, os.Stdin, os.Stdout); err != nil {
		log.Println(err)
	}
}

func runCLI(conn *client.Client, input io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(input)
	// A protocol frame body is limited to 1 KiB. Allow command syntax and headers
	// around it, but reject unbounded input before it reaches the network.
	scanner.Buffer(make([]byte, 1024), protocolInputLimit)

	_, _ = fmt.Fprintln(output, "connected; type help for commands")
	for scanner.Scan() {
		cmd, err := parseCommand(scanner.Text())
		if err != nil {
			_, _ = fmt.Fprintln(output, "invalid command; type help")
			continue
		}

		switch cmd.kind {
		case commandHelp:
			_, _ = fmt.Fprintln(output, cliHelp)
		case commandExit:
			return nil
		default:
			frames, err := buildFrames(cmd, commandChannel(cmd))
			if err != nil {
				_, _ = fmt.Fprintln(output, "cannot build request:", err)
				continue
			}
			if err := conn.SendMessage(frames...); err != nil {
				return fmt.Errorf("send request: %w", err)
			}
		}
	}
	return scanner.Err()
}

func handleReader(conn *client.Client, output io.Writer) {
	for {
		message, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(output, "response method=%q channel=%d body=%q\n", message.Method, message.Channel, message.Body.Bytes())
		if message.Method == "MESSAGE" {
			frames, err := buildAckFrames(message)
			if err == nil {
				_ = conn.SendMessage(frames...)
			}
		}
	}
}

const protocolInputLimit = 4 * 1024
