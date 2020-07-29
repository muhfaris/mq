package fmq

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// ConfigRabbitMQArgument wrap config rabbitmq
type ConfigRabbitMQArgument struct {
	Host          string
	Port          int
	AmqpURL       string
	Username      string
	Password      string
	Vhost         string
	AutoReconnect bool
	ExitOnErr     bool
	Type          string

	QueueConfig   QueueConfig
	QueueBind     QueueBindConfig
	QueueConsumer QueueConsumer
	ExchangeQueue ExchangeQueueConfig
	FnCallback    func([]byte)

	url          string
	errorChannel chan *amqp.Error
	connection   *amqp.Connection
	channel      *amqp.Channel
	closed       bool

	consumers []messageConsumer
}

// ChangeFnCallback set function callback for consumer
func (q *ConfigRabbitMQArgument) ChangeFnCallback(fn func([]byte)) error {
	if fn == nil {
		return fmt.Errorf("Function callback is empty")
	}
	q.FnCallback = fn

	return nil
}

// QueueConfig is wrap data for queue config
type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

// QueueBindConfig is wrap data for queue bind config
type QueueBindConfig struct {
	Name       string
	RoutingKey string
}

// ExchangeQueueConfig is wrap data for exhange queue config
type ExchangeQueueConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

// QueueConsumer is wrap data for queue consumer
type QueueConsumer struct {
	QueueName string
	AutoACK   bool
}

type messageConsumer func([]byte)

const (
	exchangeKeyConst = "EXCHANGE"
	routingKeyConst  = "ROUTING"

	exchangeTypeConst = "direct"
)

// NewQueue is Connection to rabbit
func NewQueue(cfg ConfigRabbitMQArgument) *ConfigRabbitMQArgument {
	queue := cfg
	// reinit
	queue.url = queue.getURL()
	queue.consumers = make([]messageConsumer, 0)

	if queue.Type == "" {
		log.Fatalln("Error client type is empty")
	}

	queue.connect()
	go queue.reconnector()
	return &queue
}

func (q *ConfigRabbitMQArgument) getURL() string {
	return amqp.URI{
		Scheme:   "amqp",
		Host:     q.Host,
		Port:     q.Port,
		Username: q.Username,
		Password: q.Password,
		Vhost:    q.Vhost,
	}.String()
}

// Send is publisher
func (q *ConfigRabbitMQArgument) Send(message []byte) error {
	err := q.channel.Publish(
		q.ExchangeQueue.Name,   // exchange
		q.QueueBind.RoutingKey, // routing key
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})
	if err != nil {
		logError("Sending message to queue failed", err)
		return err
	}

	log.Println("Sending message successfull")
	return nil
}

func (q *ConfigRabbitMQArgument) consume(consumer messageConsumer) {
	deliveries, err := q.registerQueueConsumer()
	q.executeMessageConsumer(err, consumer, deliveries, false)
}

// Consumer stream data from rabbitmq
func (q *ConfigRabbitMQArgument) Consumer() {
	stopChan := make(chan bool)
	go func() {
		q.consume(func(i []byte) {
			if q.FnCallback != nil {
				q.FnCallback(i)
			}
		})
	}()
	<-stopChan
}

// Close is close connection from rabbit
func (q *ConfigRabbitMQArgument) Close() {
	q.closed = true
	if err := q.channel.Close(); err != nil {
		logError("Error close channel:", err)
	}

	if err := q.connection.Close(); err != nil {
		logError("Error close connection:", err)
	}
}

func (q *ConfigRabbitMQArgument) reconnector() {
	for {
		err := <-q.errorChannel
		if !q.closed {
			logError("Reconnecting after connection closed", err)

			q.connect()
			if q.Type == ClientConsumerType {
				q.declareExchangeQueue()
				q.declareQueue()
				q.bindingQueue()
				q.recoverConsumers()
			}
		}
	}
}

func (q *ConfigRabbitMQArgument) connect() {
	for {
		log.Printf("Connecting to rabbitmq on %s\n", q.url)
		conn, err := amqp.Dial(q.url)
		if err == nil {
			q.connection = conn
			q.errorChannel = make(chan *amqp.Error)
			q.connection.NotifyClose(q.errorChannel)

			log.Println("Connection established!")

			q.openChannel()
			if q.Type == ClientConsumerType {
				q.declareExchangeQueue()
				q.declareQueue()
				q.bindingQueue()
			}

			return
		}

		logError("Connection to rabbitmq failed. Retrying in 1 sec... ", err)
		time.Sleep(1000 * time.Millisecond)
	}
}

func (q *ConfigRabbitMQArgument) declareExchangeQueue() {
	log.Println(q.ExchangeQueue)
	err := q.channel.ExchangeDeclare(
		q.ExchangeQueue.Name,    // name of the exchange
		q.ExchangeQueue.Type,    // type
		q.ExchangeQueue.Durable, // durable
		false,                   // delete when complete
		false,                   // internal
		false,                   // noWait
		nil,                     // arguments
	)
	logError("Exchange Declare: %s", err)
	log.Println("Declare exchange queue is successfull:", q.ExchangeQueue.Name)
}

func (q *ConfigRabbitMQArgument) declareQueue() {
	_, err := q.channel.QueueDeclare(
		q.QueueConfig.Name,    // name
		q.QueueConfig.Durable, // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)

	logError("Queue declaration failed", err)
	log.Println("Declare Queue is successfull:", q.QueueConfig.Name)
}

func (q *ConfigRabbitMQArgument) bindingQueue() {
	err := q.channel.QueueBind(
		q.QueueBind.Name,       // name of the queue
		q.QueueBind.RoutingKey, // bindingKey
		q.ExchangeQueue.Name,   // sourceExchange
		false,                  // noWait
		nil,                    // arguments
	)

	logError("Queue Bind:", err)
	log.Printf("binding queue %s", q.QueueBind.RoutingKey)
}

func (q *ConfigRabbitMQArgument) openChannel() {
	channel, err := q.connection.Channel()
	logError("Opening channel failed", err)

	q.channel = channel
	log.Println("Open channel is successfull :", q.QueueConfig.Name)
}

func (q *ConfigRabbitMQArgument) registerQueueConsumer() (<-chan amqp.Delivery, error) {
	msgs, err := q.channel.Consume(
		q.QueueConfig.Name,      // queue
		"",                      // messageConsumer
		q.QueueConsumer.AutoACK, // auto-ack
		false,                   // exclusive
		false,                   // no-local
		false,                   // no-wait
		nil,                     // args
	)

	logError("Consuming messages from queue failed", err)
	return msgs, err
}

func (q *ConfigRabbitMQArgument) executeMessageConsumer(err error, consumer messageConsumer, deliveries <-chan amqp.Delivery, isRecovery bool) {
	if err == nil {
		if !isRecovery {
			q.consumers = append(q.consumers, consumer)
		}
		go func() {
			for delivery := range deliveries {
				consumer(delivery.Body)
			}
		}()
	}
}

func (q *ConfigRabbitMQArgument) recoverConsumers() {
	for i := range q.consumers {
		var consumer = q.consumers[i]

		log.Println("Recovering consumer...")
		msgs, err := q.registerQueueConsumer()
		log.Println("Consumer recovered! Continuing message processing...")
		q.executeMessageConsumer(err, consumer, msgs, true)
	}
}

func logError(message string, err error) {
	if err != nil {
		log.Printf("%s: %s", message, err)
	}
}
