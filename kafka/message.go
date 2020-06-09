package kafka

import (
	"github.com/Shopify/sarama"
)

type Message struct {
	m *sarama.ConsumerMessage
	s sarama.ConsumerGroupSession
	q *Queue
}

func (this *Message) Value() []byte {
	if this.m != nil {
		return this.m.Value
	}
	return nil
}

func (this *Message) Key() []byte {
	if this.m != nil {
		return this.m.Key
	}
	return nil
}

func (this *Message) Topic() string {
	if this.m != nil {
		return this.m.Topic
	}
	return ""
}

func (this *Message) Message() *sarama.ConsumerMessage {
	return this.m
}

func (this *Message) Session() sarama.ConsumerGroupSession {
	return this.s
}

func (this *Message) Ack() error {
	if this.s != nil && this.m != nil {
		this.s.MarkMessage(this.m, "")
	}
	return nil
}
