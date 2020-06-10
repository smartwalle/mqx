package main

import (
	"fmt"
	"github.com/smartwalle/mx/nsq"
)

func main() {
	var config = nsq.NewConfig()
	config.NSQAddr = "localhost:4150"

	var q, err = nsq.New("topic-1", "channel-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < 15; i++ {
		fmt.Println(q.Enqueue([]byte(fmt.Sprintf("hello %d", i))))
	}

	select {}
}
