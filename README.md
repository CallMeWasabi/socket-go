# socket-go

## Video and Documentation
[Video presentation](https://youtu.be/uowZDmnExSE)
[Documentation](https://github.com/CallMeWasabi/socket-go/blob/main/presentation/socket_go_document.pdf)
[Presentation Slide](https://github.com/CallMeWasabi/socket-go/blob/main/presentation/socket_go_present.pdf)

## Run the protocol CLI

Start the server:

```bash
go run ./cmd/server
```

In another terminal, start the client:

```bash
go run ./cmd/client --addr localhost:8080
```

Subscribe first (this creates the topic), then publish from another client:

```text
subscribe orders
```

```text
publish orders hello
```

Available client commands:

```text
publish <topic> <payload>
subscribe <topic>
unsubscribe <topic>
topics
ping
help
exit
```

The client encodes every remote command as protocol frames. Routing and
responses are handled by the server's `Handler` implementation. Incoming
`MESSAGE` deliveries are acknowledged automatically by the CLI. The broker
uses at-least-once delivery: an unacknowledged delivery is retried and may be
seen more than once.

The in-memory topic queue is bounded (128 pending messages by default), and a
consumer has one in-flight delivery by default. Use `core.NewExchangeWithConfig`
to tune the queue, retry interval, retry limit, and in-flight window.
