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

func (this *AsyncProducer) handleSuccesses() {
	defer this.wg.Done()
	for msg := range this.producer.Successes() {
		if this.Completion != nil {
			this.Completion(msg, nil)
		}
	}
}

func (this *AsyncProducer) handleErrors() {
	defer this.wg.Done()
	for err := range this.producer.Errors() {
		if this.Completion != nil {
			this.Completion(err.Msg, err.Err)
		}
	}
}

func (this *AsyncProducer) Enqueue(data []byte) error {
	var m = &sarama.ProducerMessage{}
	m.Topic = this.topic
	//m.Partition =
	//m.Key =
	m.Value = sarama.ByteEncoder(data)
	return this.EnqueueMessage(m)
}

func (this *AsyncProducer) EnqueueMessage(m *sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	m.Topic = this.topic
	this.producer.Input() <- m
	return nil
}

func (this *AsyncProducer) MultiEnqueue(data ...[]byte) error {
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

func (this *AsyncProducer) EnqueueMessages(m ...*sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	for _, ele := range m {
		this.producer.Input() <- ele
	}
	return nil
}

func (this *AsyncProducer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.producer != nil {
		this.producer.AsyncClose()
		this.wg.Wait()
		this.producer = nil
	}

	return nil
}
