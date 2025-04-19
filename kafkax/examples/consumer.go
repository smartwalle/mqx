package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/mx/kafkax"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = kafkax.NewConfig()
	config.Reader.Brokers = []string{"192.168.1.4:9092"}
	var consumer = kafkax.NewConsumer("topic-1", "group-1", config, func(ctx context.Context, message kafkax.Message) bool {
		fmt.Println(message.Topic, string(message.Value))
		return true
	})

	defer consumer.Stop(context.Background())

	if err := consumer.Start(context.Background()); err != nil {
		fmt.Println("Start Error:", err)
	}
	fmt.Println("消费者准备就绪")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	}
}
