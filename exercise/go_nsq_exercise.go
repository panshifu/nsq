package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"sync"
	"time"
	//"sync"
)

func main() {
	waiter := sync.WaitGroup{}
	waiter.Add(1)
	//go consumer()
	go producer("node1", "192.168.51.212:32779")
	go producer("node2", "192.168.51.212:32777")
	//producer("node1", "192.168.51.212:32777")
	go consumer("192.168.51.212:32779")
	go consumer("192.168.51.212:32777")
	//time.Sleep(time.Second * 5)

	waiter.Wait()

}

func producer(tag string, addr string) {
	config := nsq.NewConfig()
	p, err := nsq.NewProducer(addr, config)
	if err != nil {
		panic(err)
	}
	for {
		time.Sleep(time.Second * 5)
		if err := p.Publish("test", []byte(tag+":"+time.Now().String())); err != nil {
			panic(err)
		}
	}
}

type ConsumerT struct{}

func (*ConsumerT) HandleMessage(msg *nsq.Message) error {
	fmt.Println(string(msg.Body))
	return nil
}

func consumer(addr string) {
	config := nsq.NewConfig()
	c, err := nsq.NewConsumer("test", "test-channel", config)
	if err != nil {
		panic(err)
	}
	//hand := func(msg *nsq.Message) error {
	//	fmt.Println(string(msg.Body))
	//	return nil
	//}
	//c.AddHandler(nsq.HandlerFunc(hand))
	c.AddHandler(&ConsumerT{})
	if err := c.ConnectToNSQD(addr); err != nil {
		fmt.Println(err)
	}
}
