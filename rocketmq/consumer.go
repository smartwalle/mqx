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

func (c *Consumer) Dequeue(handler mx.Handler) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return mx.ErrClosedQueue
	}

	if c.consumer != nil {
		c.consumer.Unsubscribe(c.topic)
		c.consumer.Shutdown()
		c.consumer = nil
	}

	if c.consumer == nil {
		var opts []consumer.Option
		opts = append(opts, consumer.WithGroupName(c.group))
		opts = append(opts, consumer.WithInstance(c.config.InstanceName))
		opts = append(opts, consumer.WithNameServer(c.config.NameServerAddrs))
		opts = append(opts, consumer.WithNameServerDomain(c.config.NameServerDomain))
		opts = append(opts, consumer.WithNamespace(c.config.Namespace))
		opts = append(opts, consumer.WithVIPChannel(c.config.VIPChannelEnabled))
		opts = append(opts, consumer.WithRetry(c.config.RetryTimes))
		opts = append(opts, consumer.WithCredentials(c.config.Credentials))

		opts = append(opts, consumer.WithConsumerModel(c.config.Consumer.ConsumerModel))
		opts = append(opts, consumer.WithConsumeFromWhere(c.config.Consumer.FromWhere))
		opts = append(opts, consumer.WithConsumerOrder(c.config.Consumer.ConsumeOrderly))
		opts = append(opts, consumer.WithConsumeMessageBatchMaxSize(c.config.Consumer.ConsumeMessageBatchMaxSize))
		opts = append(opts, consumer.WithInterceptor(c.config.Consumer.Interceptors...))
		opts = append(opts, consumer.WithMaxReconsumeTimes(c.config.Consumer.MaxReconsumeTimes))
		opts = append(opts, consumer.WithStrategy(c.config.Consumer.Strategy))
		opts = append(opts, consumer.WithPullBatchSize(c.config.Consumer.PullBatchSize))
		opts = append(opts, consumer.WithRebalanceLockInterval(c.config.Consumer.RebalanceLockInterval))
		opts = append(opts, consumer.WithAutoCommit(c.config.Consumer.AutoCommit))
		opts = append(opts, consumer.WithSuspendCurrentQueueTimeMillis(c.config.Consumer.SuspendCurrentQueueTimeMillis))

		consumer, err := rocketmq.NewPushConsumer(opts...)
		if err != nil {
			return err
		}
		c.consumer = consumer
	}

	c.consumer.Subscribe(c.topic, consumer.MessageSelector{}, func(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range messages {
			var m = &Message{}
			m.m = msg
			if handler(m) {
				return consumer.ConsumeSuccess, nil
			}
		}
		return consumer.ConsumeRetryLater, nil
	})
	c.consumer.Start()
	return nil
}

func (c *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}

	if c.consumer != nil {
		c.consumer.Unsubscribe(c.topic)
		if err := c.consumer.Shutdown(); err != nil {
			return err
		}
		c.consumer = nil
	}

	return nil
}
