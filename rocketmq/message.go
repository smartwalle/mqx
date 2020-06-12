package rocketmq

import "github.com/apache/rocketmq-client-go/v2/primitive"

type Message struct {
	m *primitive.MessageExt
}

func (this *Message) Value() []byte {
	if this.m != nil {
		return this.m.Body
	}
	return nil
}
