package kafka

import (
	"github.com/IBM/sarama"
	"github.com/smartwalle/mx"
	"sync"
	"sync/atomic"
)

type AsyncProducer struct {
	closed     int32
	topic      string
	client     sarama.Client
	producer   sarama.AsyncProducer
	wg         sync.WaitGroup
	Completion func(message *sarama.ProducerMessage, err error)
}

func NewAsyncProducer(topic string, config *Config) (*AsyncProducer, error) {
	client, err := sarama.NewClient(config.Addrs, config.Config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	var p = &AsyncProducer{}
	p.closed = 0
	p.topic = topic
	p.client = client
	p.producer = producer
	p.Completion = config.Completion
	p.wg.Add(2)
	go p.handleSuccesses()
	go p.handleErrors()

	return p, nil
}

func (p *AsyncProducer) handleSuccesses() {
	defer p.wg.Done()
	for msg := range p.producer.Successes() {
		if p.Completion != nil {
			p.Completion(msg, nil)
		}
	}
}

func (p *AsyncProducer) handleErrors() {
	defer p.wg.Done()
	for err := range p.producer.Errors() {
		if p.Completion != nil {
			p.Completion(err.Msg, err.Err)
		}
	}
}

func (p *AsyncProducer) Enqueue(data []byte) error {
	var m = &sarama.ProducerMessage{}
	m.Topic = p.topic
	//m.Partition =
	//m.Key =
	m.Value = sarama.ByteEncoder(data)
	return p.EnqueueMessage(m)
}

func (p *AsyncProducer) EnqueueMessage(m *sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}

	m.Topic = p.topic
	p.producer.Input() <- m
	return nil
}

func (p *AsyncProducer) MultiEnqueue(data ...[]byte) error {
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

func (p *AsyncProducer) EnqueueMessages(m ...*sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}

	for _, ele := range m {
		p.producer.Input() <- ele
	}
	return nil
}

func (p *AsyncProducer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.producer != nil {
		p.producer.AsyncClose()
		p.wg.Wait()
	}

	return nil
}
