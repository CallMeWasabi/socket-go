package main

import (
	"bufio"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	servAddr := "localhost:8080"
	tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
	if err != nil {
		log.Fatalln("Resolve tcp error:", err.Error())
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Fatalln("Dial failed:", err.Error())
	}

	// frame := protocol.NewFrame(1, 255)
	// frame.WriteString("Hello World!")
	// bytes, _ := frame.Encode()
	// log.Println(string(bytes), bytes)

	go handleReader(conn)
	go handleWriter(conn)

}

func handleWriter(conn net.Conn) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		if scanner.Scan() {
			input := scanner.Text()

			input = strings.TrimSpace(input)
			if input == "exit" {
				break
			}

			_, err := conn.Write([]byte(input))
			if err != nil {
				log.Println("Write to server failed:", err.Error())
				continue
			}
		}
	}

	conn.Close()
}

func handleReader(conn net.Conn) {
	for {
		reply := make([]byte, 1024)

		_, err := conn.Read(reply)
		if err != nil {
			log.Fatalln("Read from server failed:", err.Error())
		}

		println("Reply from server=", string(reply))
	}
}
