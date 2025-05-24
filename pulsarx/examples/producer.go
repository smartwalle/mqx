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
	producer, err := pulsarx.NewProducer("topic-1", config)
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
