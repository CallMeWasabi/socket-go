package core

import (
	"time"

	"github.com/google/uuid"
)

type RawMessage struct {
	ID                 uuid.UUID
	Topic              string
	Content            string
	PublisherChannelID uuid.UUID
	Attempt            int64
}

type DeliveryMessage struct {
	Meta    DeliveryMeta
	Content []byte
}

type DeliveryMeta struct {
	Topic         string
	MessageID     uuid.UUID
	DeliveryID    uuid.UUID
	SendChannelID uuid.UUID // sender channel id
	RecvChannelID uuid.UUID // reciever channel id
	ConsumerID    uuid.UUID
	Attempt       int64
	Deadline      time.Time
}

func NewDeliveryMessage(r *RawMessage, c *Channel) *DeliveryMessage {
	attempt := r.Attempt
	if attempt <= 0 {
		attempt = 1
	}
	return newDeliveryMessage(r, c, attempt)
}

func newDeliveryMessage(r *RawMessage, c *Channel, attempt int64) *DeliveryMessage {
	messageID := r.ID
	if messageID == uuid.Nil {
		messageID = uuid.New()
	}
	return &DeliveryMessage{
		Meta: DeliveryMeta{
			Topic:         r.Topic,
			MessageID:     messageID,
			DeliveryID:    uuid.New(),
			SendChannelID: r.PublisherChannelID,
			RecvChannelID: c.ID,
			ConsumerID:    c.ConsumerID,
			Attempt:       attempt,
			Deadline:      time.Now().Add(time.Minute),
		},
		Content: []byte(r.Content),
	}
}
