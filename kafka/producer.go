package kafka

import (
	"github.com/IBM/sarama"
	"github.com/smartwalle/mx"
	"sync/atomic"
)

type Producer struct {
	closed   int32
	topic    string
	client   sarama.Client
	producer sarama.SyncProducer
}

func NewProducer(topic string, config *Config) (*Producer, error) {
	client, err := sarama.NewClient(config.Addrs, config.Config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	var p = &Producer{}
	p.closed = 0
	p.topic = topic
	p.client = client
	p.producer = producer
	return p, nil
}

func (this *Producer) Enqueue(data []byte) error {
	var m = &sarama.ProducerMessage{}
	m.Topic = this.topic
	//m.Partition =
	//m.Key =
	m.Value = sarama.ByteEncoder(data)
	return this.EnqueueMessage(m)
}

func (this *Producer) EnqueueMessage(m *sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	m.Topic = this.topic
	_, _, err := this.producer.SendMessage(m)
	return err
}

func (this *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	var ms = make([]*sarama.ProducerMessage, 0, len(data))
	for _, d := range data {
		var m = &sarama.ProducerMessage{}
		m.Topic = this.topic
		m.Value = sarama.ByteEncoder(d)
		ms = append(ms, m)
	}
	return this.EnqueueMessages(ms...)
}

func (this *Producer) EnqueueMessages(m ...*sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	return this.producer.SendMessages(m)
}

func (this *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.producer != nil {
		if err := this.producer.Close(); err != nil {
			return err
		}
		this.producer = nil
	}

	return nil
}
