package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"time"
	"sync"
)

func main() {
	waiter := sync.WaitGroup{}
	waiter.Add(1)
	go consumer()
	go producer("node1", "192.168.51.212:32779")
	//go producer("node1", "192.168.51.212:32777")

	waiter.Wait()

}

func producer(tag string, addr string) {
	config := nsq.NewConfig()
	p, err := nsq.NewProducer(addr, config)
	if err != nil {
		panic(err)
	}
	//for {
	time.Sleep(time.Second * 5)
	p.Publish("test", []byte(tag+":"+time.Now().String()))
	//}
}

func consumer() {
	config := nsq.NewConfig()
	c, err := nsq.NewConsumer("test", "consumer", config)
	if err != nil {
		panic(err)
	}
	hand := func(msg *nsq.Message) error {
		fmt.Println(string(msg.Body))
		return nil
	}
	c.AddHandler(nsq.HandlerFunc(hand))
	if err := c.ConnectToNSQLookupd("192.168.51.212:32774"); err != nil {
		fmt.Println(err)
	}
}
