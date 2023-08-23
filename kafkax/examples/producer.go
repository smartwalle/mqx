package main

import (
	"fmt"
	"github.com/smartwalle/mx/kafkax"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = kafkax.NewConfig()
	config.Writer.Addr = kafkax.TCP("192.168.1.99:9092")
	config.Writer.Async = true
	p, err := kafkax.NewProducer("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("begin...")
	for i := 0; i < 100000; i++ {
		if err := p.Enqueue([]byte(fmt.Sprintf("hello %d", i))); err != nil {
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
	fmt.Println("Close", p.Close())
}
