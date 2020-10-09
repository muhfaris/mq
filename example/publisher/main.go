package main

import (
	"fmt"
	"log"
	"time"

	"github.com/muhfaris/mq"
	"github.com/streadway/amqp"
)

func main() {
	config := mq.ConfigRabbitMQArgument{
		Name:     "publisher_rule_prepare",
		Schema:   "amqp",
		Host:     "localhost",
		Username: "admin",
		Password: "admin",
		Vhost:    "/",
		Port:     5672,
		Type:     mq.ClientProducerType,

		PublisherConfig: mq.PublisherConfig{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
		},

		QueueConfig: mq.QueueConfig{
			Name:       "insight_data",
			Durable:    true,
			AutoDelete: false,
		},

		QueueBind: mq.QueueBindConfig{
			Name:       "insight_data",
			RoutingKey: "insight_data",
		},

		ExchangeQueue: mq.ExchangeQueueConfig{
			Name:       "insight_data",
			Type:       "direct",
			Durable:    true,
			AutoDelete: false,
		},
	}

	session, err := mq.NewQueue(config)
	if err != nil {
		log.Println("error new queue:", err)
		return
	}

	var index int
	for {
		index++
		message := []byte(fmt.Sprintf("HELLO, %d", index))
		time.Sleep(time.Second * 3)
		if err := session.Push(message); err != nil {
			fmt.Printf("Push failed: %s\n", err)
		} else {
			fmt.Println("Push succeeded!")
		}
	}
}
