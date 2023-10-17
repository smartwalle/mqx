package kafkax

import (
	"github.com/segmentio/kafka-go"
)

type Message struct {
	m kafka.Message
}

func (m *Message) Value() []byte {
	return m.m.Value
}

func (m *Message) Topic() string {
	return m.m.Topic
}

func (m *Message) Message() kafka.Message {
	return m.m
}
