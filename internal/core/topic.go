package core

import "github.com/google/uuid"

type Topic struct {
	assign   chan *Consumer
	unassign chan *Consumer
	messages chan *Message

	name string

	consumers map[uuid.UUID]*Consumer
}

func newTopic(name string) *Topic {
	return &Topic{
		assign:   make(chan *Consumer),
		unassign: make(chan *Consumer),
		messages: make(chan *Message, 128),

		name: name,

		consumers: make(map[uuid.UUID]*Consumer),
	}
}

func (t *Topic) Run() {
	for {
		select {
		case c := <-t.assign:
			if _, ok := t.consumers[c.ID]; ok {
				continue
			}

			t.consumers[c.ID] = c
		case c := <-t.unassign:
			if _, ok := t.consumers[c.ID]; !ok {
				continue
			}

			delete(t.consumers, c.ID)
		case msg := <-t.messages:
			for _, c := range t.consumers {
				c.message <- msg
			}
		}
	}
}
