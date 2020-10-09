# Message RabbitMQ Client
This library wrap from [github.com/streadway/amqp](streadway/amqp). Supporting reconnect if connection lost, easy configuration,
support `SIGINT`, `SIGTERM`, `SIGQUIT`, `SIGSTOP` and closes connection

Note: I use fro production.

### Config
Valid type config below, it is case-sensitive.
- PRODUCER
- CONSUMER

### Example
This example for manual confirmation message and prefetch count 1 per consumer.

#### Producer
```
// producer.go
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
}```

### Consumer
```
// config consumer.go
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
				log.Println("MESSAGE:", string(d.Body))
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
```
