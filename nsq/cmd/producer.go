package main

import (
	"fmt"
	"github.com/smartwalle/mx/nsq"
	"time"
)

func main() {
	var port = "4150"

	var config = nsq.NewConfig()
	config.NSQAddr = "localhost:" + port

	var q, err = nsq.New("topic-1", "channel-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < 10000000; i++ {
		fmt.Println(q.Enqueue([]byte(fmt.Sprintf("hello %s - %d", port, i))))
		time.Sleep(time.Second * 1)
	}

	select {}
}
