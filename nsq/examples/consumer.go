package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/mx/nsq"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = nsq.NewConfig()
	config.NSQAddr = "127.0.0.1:4150"
	config.NSQLookupAddrs = []string{"127.0.0.1:4161"}
	var consumer = nsq.NewConsumer("topic-1", "channel-1", config, func(ctx context.Context, message *nsq.Message) error {
		fmt.Println(string(message.Body))
		return nil
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
