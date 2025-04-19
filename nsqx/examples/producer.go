package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/mx/nsqx"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = nsqx.NewConfig()
	config.NSQAddr = "127.0.0.1:4150"
	config.NSQLookupAddrs = []string{"127.0.0.1:4161"}
	producer, err := nsqx.NewProducer("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer producer.Close()

	fmt.Println("begin...")
	for i := 0; i < 1000; i++ {
		if err := producer.Enqueue(context.Background(), []byte(fmt.Sprintf("hello %d", i))); err != nil {
			fmt.Println("Enqueue", err)
			break
		}
	}
	fmt.Println("end...")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	}
}
