package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/mqx/pulsarx"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = pulsarx.NewConfig()
	var consumer = pulsarx.NewConsumer("topic-1", "channel-1", config, func(ctx context.Context, message pulsarx.Message) bool {
		fmt.Println(message.Topic(), string(message.Payload()))
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
