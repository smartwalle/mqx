package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/mqx/kafkax"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = kafkax.NewConfig()
	config.Writer.Addr = kafkax.TCP("192.168.1.4:9092")
	config.Writer.Async = true
	producer := kafkax.NewProducer("topic-1", config)

	fmt.Println("begin...")
	for i := 0; i < 10; i++ {
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
	fmt.Println("Close", producer.Close())
}
