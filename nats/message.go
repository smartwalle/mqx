package nats

import (
	n "github.com/nats-io/nats.go"
)

type Message struct {
	m *n.Msg
}

func (m *Message) Value() []byte {
	if m.m != nil {
		return m.m.Data
	}
	return nil
}

func (m *Message) Topic() string {
	if m.m != nil {
		return m.m.Subject
	}
	return ""
}

func (m *Message) Message() *n.Msg {
	return m.m
}
