package kafka

import (
	"github.com/IBM/sarama"
)

type Message struct {
	m *sarama.ConsumerMessage
}

func (m *Message) Value() []byte {
	if m.m != nil {
		return m.m.Value
	}
	return nil
}

func (m *Message) Topic() string {
	if m.m != nil {
		return m.m.Topic
	}
	return ""
}

func (m *Message) Message() *sarama.ConsumerMessage {
	return m.m
}
