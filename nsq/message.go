package nsq

import "github.com/nsqio/go-nsq"

type Message struct {
	m *nsq.Message
}

func (this *Message) Value() []byte {
	if this.m != nil {
		return this.m.Body
	}
	return nil
}

func (this *Message) Message() *nsq.Message {
	return this.m
}
