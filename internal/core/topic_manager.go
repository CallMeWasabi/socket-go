package core

import (
	"fmt"
)

const DefaultPartitionSize = 1

type TopicManager struct {
	topics map[string]*Topic
}

func newTopicManager() *TopicManager {
	return &TopicManager{
		topics: make(map[string]*Topic),
	}
}

func (tm *TopicManager) assignConsumer(cmd *SubscribeCmd) {
	name := cmd.Topic
	topic, ok := tm.topics[name]
	if !ok {
		topic = newTopic(name)
		tm.topics[name] = topic

		go topic.Run()
	}

	topic.assign <- cmd.C
}

func (tm *TopicManager) unassignConsumer(cmd *UnsubscribeCmd) {
	name := cmd.Topic
	topic, ok := tm.topics[name]
	if !ok {
		return
	}

	topic.unassign <- cmd.C
}

func (tm *TopicManager) publish(msg *Message) error {
	name := msg.Topic
	topic, ok := tm.topics[name]
	if !ok {
		return fmt.Errorf("Not found topic %s\n", name)
	}

	topic.messages <- msg
	return nil
}
