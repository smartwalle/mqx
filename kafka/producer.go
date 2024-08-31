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

func (p *Producer) Enqueue(data []byte) error {
	var m = &sarama.ProducerMessage{}
	m.Topic = p.topic
	//m.Partition =
	//m.Key =
	m.Value = sarama.ByteEncoder(data)
	return p.EnqueueMessage(m)
}

func (p *Producer) EnqueueMessage(m *sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}

	m.Topic = p.topic
	_, _, err := p.producer.SendMessage(m)
	return err
}

func (p *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	var ms = make([]*sarama.ProducerMessage, 0, len(data))
	for _, d := range data {
		var m = &sarama.ProducerMessage{}
		m.Topic = p.topic
		m.Value = sarama.ByteEncoder(d)
		ms = append(ms, m)
	}
	return p.EnqueueMessages(ms...)
}

func (p *Producer) EnqueueMessages(m ...*sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}

	return p.producer.SendMessages(m)
}

func (p *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			return err
		}
		p.producer = nil
	}

	return nil
}
