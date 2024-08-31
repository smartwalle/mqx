package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

type Message struct {
	m pulsar.Message
}

func (m *Message) Value() []byte {
	if m.m != nil {
		return m.m.Payload()
	}
	return nil
}

func (m *Message) Topic() string {
	if m.m != nil {
		return m.m.Topic()
	}
	return ""
}

func (m *Message) Message() pulsar.Message {
	return m.m
}

func NewProducerMessage() *pulsar.ProducerMessage {
	return &pulsar.ProducerMessage{}
}
