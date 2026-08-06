package core

import (
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
)

// TODO: support multithread safe
type Zookeeper struct {
	Topics map[string][]*Consumer
}

func (z *Zookeeper) HasTopic(topic string) bool {
	_, ok := z.Topics[topic]
	return ok
}

func (z *Zookeeper) AddTopic(topic string) {
	if _, ok := z.Topics[topic]; ok {
		return
	}

	z.Topics[topic] = []*Consumer{}
}

func (z *Zookeeper) RemoveTopic(topic string) {
	if _, ok := z.Topics[topic]; !ok {
		return
	}

	delete(z.Topics, topic)
}

func (z *Zookeeper) AddConsumer(c *Consumer, topic string) error {
	if !z.HasTopic(topic) {
		return errors.New(fmt.Sprint("not found topic: ", topic))
	} else if slices.ContainsFunc(z.Topics[topic], func(iterC *Consumer) bool {
		return iterC.ID == c.ID
	}) {
		return nil
	}

	z.Topics[topic] = append(z.Topics[topic], c)

	return nil
}

func (z *Zookeeper) RemoveConsumer(id uuid.UUID, topic string) error {
	if !z.HasTopic(topic) {
		return errors.New(fmt.Sprint("not found topic: ", topic))
	}

	idx := slices.IndexFunc(z.Topics[topic], func(c *Consumer) bool {
		return c.ID == id
	})
	if idx == -1 {
		return nil
	}

	// order of consumer doesn't matter
	consumers := z.Topics[topic]
	consumers[idx] = consumers[len(consumers)-1]
	consumers = consumers[:len(consumers)-1]

	z.Topics[topic] = consumers

	return nil
}
