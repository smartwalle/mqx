package rocketmq

type Message struct {
}

func (this *Message) Value() []byte {
	return nil
}

func (this *Message) Ack() error {
	return nil
}
