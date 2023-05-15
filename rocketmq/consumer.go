package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/smartwalle/mx"
	"sync/atomic"
)

type Consumer struct {
	closed   int32
	topic    string
	group    string
	config   *Config
	consumer rocketmq.PushConsumer
}

func NewConsumer(topic, group string, config *Config) (*Consumer, error) {
	var c = &Consumer{}
	c.closed = 0
	c.topic = topic
	c.group = group
	c.config = config
	return c, nil
}

func (this *Consumer) Dequeue(handler mx.Handler) error {
	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	if this.consumer != nil {
		this.consumer.Unsubscribe(this.topic)
		this.consumer.Shutdown()
		this.consumer = nil
	}

	if this.consumer == nil {
		var opts []consumer.Option
		opts = append(opts, consumer.WithGroupName(this.group))
		opts = append(opts, consumer.WithInstance(this.config.InstanceName))
		opts = append(opts, consumer.WithNameServer(this.config.NameServerAddrs))
		opts = append(opts, consumer.WithNameServerDomain(this.config.NameServerDomain))
		opts = append(opts, consumer.WithNamespace(this.config.Namespace))
		opts = append(opts, consumer.WithVIPChannel(this.config.VIPChannelEnabled))
		opts = append(opts, consumer.WithRetry(this.config.RetryTimes))
		opts = append(opts, consumer.WithCredentials(this.config.Credentials))

		opts = append(opts, consumer.WithConsumerModel(this.config.Consumer.ConsumerModel))
		opts = append(opts, consumer.WithConsumeFromWhere(this.config.Consumer.FromWhere))
		opts = append(opts, consumer.WithConsumerOrder(this.config.Consumer.ConsumeOrderly))
		opts = append(opts, consumer.WithConsumeMessageBatchMaxSize(this.config.Consumer.ConsumeMessageBatchMaxSize))
		opts = append(opts, consumer.WithInterceptor(this.config.Consumer.Interceptors...))
		opts = append(opts, consumer.WithMaxReconsumeTimes(this.config.Consumer.MaxReconsumeTimes))
		opts = append(opts, consumer.WithStrategy(this.config.Consumer.Strategy))
		opts = append(opts, consumer.WithPullBatchSize(this.config.Consumer.PullBatchSize))
		opts = append(opts, consumer.WithRebalanceLockInterval(this.config.Consumer.RebalanceLockInterval))
		opts = append(opts, consumer.WithAutoCommit(this.config.Consumer.AutoCommit))
		opts = append(opts, consumer.WithSuspendCurrentQueueTimeMillis(this.config.Consumer.SuspendCurrentQueueTimeMillis))

		consumer, err := rocketmq.NewPushConsumer(opts...)
		if err != nil {
			return err
		}
		this.consumer = consumer
	}

	this.consumer.Subscribe(this.topic, consumer.MessageSelector{}, func(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range messages {
			var m = &Message{}
			m.m = msg
			if handler(m) {
				return consumer.ConsumeSuccess, nil
			}
		}
		return consumer.ConsumeRetryLater, nil
	})
	this.consumer.Start()
	return nil
}

func (this *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.consumer != nil {
		this.consumer.Unsubscribe(this.topic)
		if err := this.consumer.Shutdown(); err != nil {
			return err
		}
		this.consumer = nil
	}

	return nil
}
