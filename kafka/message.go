package kafka

import (
	"github.com/IBM/sarama"
)

type Message struct {
	m *sarama.ConsumerMessage
}

func (this *Message) Value() []byte {
	if this.m != nil {
		return this.m.Value
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
