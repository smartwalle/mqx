package main

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/smartwalle/mx"
	"github.com/smartwalle/mx/rocketmq"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var config = rocketmq.NewConfig()
	config.Consumer.FromWhere = consumer.ConsumeFromFirstOffset
	config.Consumer.ConsumeOrderly = true
	q, err := rocketmq.New("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	q.Dequeue("group-1", func(m mx.Message) bool {
		var mm = m.(*rocketmq.Message)
		fmt.Println("Dequeue", mm.Message().Queue.QueueId, time.Now(), string(mm.Value()))
		return true
	})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	}
	fmt.Println("Close", q.Close())
}
