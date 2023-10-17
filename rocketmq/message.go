package rocketmq

import "github.com/apache/rocketmq-client-go/v2/primitive"

type Message struct {
	m *primitive.MessageExt
}

func (m *Message) Value() []byte {
	if m.m != nil {
		return m.m.Body
	}
	return nil
}

func (m *Message) Topic() string {
	if m.m != nil {
		return m.m.Topic
	}
	return ""
}

func (m *Message) Message() *primitive.MessageExt {
	return m.m
}
