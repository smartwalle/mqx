package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/smartwalle/mx"
	"sync"
)

type Queue struct {
	mu            *sync.Mutex
	closed        bool
	topic         string
	group         string
	client        sarama.Client
	producer      sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	consumer      *consumer
}

func New(topic, group string, client sarama.Client) (mx.Queue, error) {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	var q = &Queue{}
	q.mu = &sync.Mutex{}
	q.closed = false
	q.topic = topic
	q.group = group
	q.client = client
	q.producer = producer
	q.asyncProducer = asyncProducer
	return q, nil
}

func (this *Queue) Enqueue(value []byte) error {
	var m = &sarama.ProducerMessage{}
	m.Topic = this.topic
	//m.Partition =
	//m.Key =
	m.Value = sarama.ByteEncoder(value)
	return this.EnqueueMessage(m)
}

func (this *Queue) EnqueueMessage(m *sarama.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if this.closed {
		return mx.ErrClosedQueue
	}

	_, _, err := this.producer.SendMessage(m)
	return err
}

//func (this *Queue) AsyncEnqueue(value []byte, h func(error)) {
//	var m = &sarama.ProducerMessage{}
//	m.Topic = this.topic
//	//m.Partition =
//	//m.Key =
//	m.Value = sarama.ByteEncoder(value)
//	this.AsyncEnqueueMessage(m, h)
//}
//
//func (this *Queue) AsyncEnqueueMessage(m *sarama.ProducerMessage, h func(error)) {
//	if m == nil {
//		return
//	}
//
//	if this.closed {
//		return
//	}
//
//	this.asyncProducer.Input() <- m
//
//	select {
//	case <-this.asyncProducer.Successes():
//		if h != nil {
//			h(nil)
//		}
//	case err := <-this.asyncProducer.Errors():
//		if h != nil {
//			h(err)
//		}
//	}
//}

func (this *Queue) Dequeue(h mx.Handler) error {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
		return mx.ErrClosedQueue
	}

	if this.consumer != nil {
		this.consumer.Close()
		<-this.consumer.StopChan
		this.consumer = nil
	}

	if this.consumer == nil {
		consumer, err := newConsumer(this.topic, this.group, this.client, h)
		if err != nil {
			this.mu.Unlock()
			return err
		}
		this.consumer = consumer
	}
	this.mu.Unlock()

	return nil
}

func (this *Queue) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return nil
	}
	this.closed = true

	if this.consumer != nil {
		var err = this.consumer.Close()
		<-this.consumer.StopChan
		if err != nil {
			return err
		}
	}

	if this.producer != nil {
		var err = this.producer.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
