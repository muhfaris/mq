package main

import (
	"log"

	"github.com/muhfaris/mq"
)

func main() {
	config := mq.ConfigRabbitMQArgument{
		Name:     "consumer_prepare",
		Schema:   "amqp",
		Host:     "localhost",
		Username: "admin",
		Password: "admin",
		Vhost:    "/",
		Port:     5672,
		Type:     mq.ClientConsumerType,

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

		QosConfig: mq.QosConfig{
			PrefetchCount: 1,
			PrefetchSize:  0,
			Global:        false,
		},
	}

	session, err := mq.NewQueue(config)
	if err != nil {
		log.Println("error new queue:", err)
		return
	}

	stopChan := make(chan bool)
	go func() {
		msgs, err := session.Stream()
		if err != nil {
			log.Println("error stream data", err)
			return
		}

		for {
			select {
			case d := <-msgs:
				if err := d.Ack(false); err != nil {
					log.Println("error confirm message")
					return
				}

				log.Println("message confirmed!")
			}

			log.Println(" [*] Waiting for logs. To exit press CTRL+C")
		}
	}()
	<-stopChan
}
