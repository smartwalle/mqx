package nsq

import "github.com/nsqio/go-nsq"

type Message struct {
	m     *nsq.Message
	topic string
}

func (m *Message) Value() []byte {
	if m.m != nil {
		return m.m.Body
	}
	return nil
}

func (m *Message) Topic() string {
	if m.m != nil {
		return m.topic
	}
	return ""
}

func (m *Message) Message() *nsq.Message {
	return m.m
}
