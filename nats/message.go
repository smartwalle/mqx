package nats

import (
	n "github.com/nats-io/nats.go"
)

type Message struct {
	m *n.Msg
}

func (this *Message) Value() []byte {
	if this.m != nil {
		return this.m.Data
	}
	return nil
}

func (this *Message) Topic() string {
	if this.m != nil {
		return this.m.Subject
	}
	return ""
}

func (this *Message) Message() *n.Msg {
	return this.m
}
