package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/smartwalle/mx"
	"sync"
)

type consumer struct {
	mu        *sync.Mutex
	closed    bool
	readyChan chan struct{}
	StopChan  chan struct{}
	topics    []string
	cancel    context.CancelFunc
	consumer  sarama.ConsumerGroup
	handler   mx.Handler
}

func newConsumer(topic, group string, client sarama.Client, handler mx.Handler) (*consumer, error) {
	consumerGroup, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	var c = &consumer{}
	c.mu = &sync.Mutex{}
	c.closed = false
	c.readyChan = make(chan struct{})
	c.StopChan = make(chan struct{})
	c.topics = []string{topic}
	c.cancel = cancel
	c.consumer = consumerGroup
	c.handler = handler

	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumerGroup.Consume(ctx, c.topics, c); err != nil {
			}

			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}

			c.readyChan = make(chan struct{})
		}
	}()
	<-c.readyChan
	return c, nil
}

func (this *consumer) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return nil
	}
	this.closed = true
	this.cancel()
	return this.consumer.Close()
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (this *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(this.readyChan)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (this *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	if this.closed {
		close(this.StopChan)
	}
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (this *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for cm := range claim.Messages() {
		var m = &Message{}
		m.m = cm
		if ok := this.handler(m); ok {
			session.MarkMessage(cm, "")
		}
	}
	return nil
}
