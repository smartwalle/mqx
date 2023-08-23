package kafkax

import (
	"github.com/segmentio/kafka-go"
)

type Message struct {
	m kafka.Message
}

func (this *Message) Value() []byte {
	return this.m.Value
}

func (this *Message) Topic() string {
	return this.m.Topic
}

func (this *Message) Message() kafka.Message {
	return this.m
}
