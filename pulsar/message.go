package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

type Message struct {
	m pulsar.Message
}

func (this *Message) Value() []byte {
	if this.m != nil {
		return this.m.Payload()
	}
	return nil
}

func (this *Message) Topic() string {
	if this.m != nil {
		return this.m.Topic()
	}
	return ""
}

func (this *Message) Message() pulsar.Message {
	return this.m
}
