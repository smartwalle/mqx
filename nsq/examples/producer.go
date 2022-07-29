package main

import (
	"fmt"
	"github.com/smartwalle/mx/nsq"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = nsq.NewConfig()
	config.NSQAddr = "192.168.1.77:4150"
	config.NSQLookupAddrs = []string{"192.168.1.77:4161"}
	p, err := nsq.NewProducer("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("begin...")
	for i := 0; i < 1000; i++ {
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
