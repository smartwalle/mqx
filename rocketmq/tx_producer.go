package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/smartwalle/mx"
	"sync/atomic"
)

type TxProducer struct {
	closed   int32
	config   *Config
	producer rocketmq.TransactionProducer
}

func NewTxProducer(listener primitive.TransactionListener, config *Config) (*TxProducer, error) {
	var opts []producer.Option
	opts = append(opts, producer.WithGroupName(config.Producer.Group))
	opts = append(opts, producer.WithInstanceName(config.InstanceName))
	opts = append(opts, producer.WithNameServer(config.NameServerAddrs))
	opts = append(opts, producer.WithNameServerDomain(config.NameServerDomain))
	opts = append(opts, producer.WithNamespace(config.Namespace))
	opts = append(opts, producer.WithVIPChannel(config.VIPChannelEnabled))
	opts = append(opts, producer.WithRetry(config.RetryTimes))
	opts = append(opts, producer.WithCredentials(config.Credentials))

	opts = append(opts, producer.WithInterceptor(config.Producer.Interceptors...))
	opts = append(opts, producer.WithSendMsgTimeout(config.Producer.SendMsgTimeout))
	opts = append(opts, producer.WithQueueSelector(config.Producer.Selector))
	opts = append(opts, producer.WithDefaultTopicQueueNums(config.Producer.DefaultTopicQueueNums))
	opts = append(opts, producer.WithCreateTopicKey(config.Producer.CreateTopicKey))

	var producer, err = rocketmq.NewTransactionProducer(listener, opts...)
	if err != nil {
		return nil, err
	}

	if err = producer.Start(); err != nil {
		return nil, err
	}

	var q = &TxProducer{}
	q.closed = 0
	q.config = config
	q.producer = producer
	return q, nil
}

func (p *TxProducer) Enqueue(topic string, value []byte, properties map[string]string) (*primitive.TransactionSendResult, error) {
	var m = primitive.NewMessage(topic, value)
	m.WithProperties(properties)
	return p.EnqueueMessage(m)
}

func (p *TxProducer) EnqueueMessage(m *primitive.Message) (*primitive.TransactionSendResult, error) {
	if m == nil {
		return nil, nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, mx.ErrClosedQueue
	}

	return p.producer.SendMessageInTransaction(context.Background(), m)
}

func (p *TxProducer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.producer != nil {
		if err := p.producer.Shutdown(); err != nil {
			return err
		}
	}

	return nil
}
