package fmq

import (
	"fmt"
	"log"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// ConfigRabbitMQArgument wrap config rabbitmq
type ConfigRabbitMQArgument struct {
	Host          string
	Port          int
	AMQPURL       string
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
	log       *logrus.Logger
}

// ChangeFnCallback set function callback for consumer
func (q *ConfigRabbitMQArgument) ChangeFnCallback(fn func([]byte)) error {
	if fn == nil {
		return fmt.Errorf("function callback is empty")
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
func NewQueue(cfg ConfigRabbitMQArgument) (*ConfigRabbitMQArgument, error) {
	queue := new(ConfigRabbitMQArgument)
	queue = &cfg

	log.Println(cfg)
	url := cfg.getURL()
	err := queue.changeURL(url)
	if err != nil {
		return &ConfigRabbitMQArgument{}, fmt.Errorf("error config is empty")
	}

	if !MessageQueueTypeValid(cfg.Type) {
		return &ConfigRabbitMQArgument{}, fmt.Errorf("invalid type of configuration")
	}

	queue.logger()
	queue.changeConsumer(0)
	queue.connect()
	go queue.reconnector()

	return queue, nil
}

func (q *ConfigRabbitMQArgument) changeURL(url string) error {
	if url == "" {
		return fmt.Errorf("error config is empty")
	}

	q.url = url
	return nil
}

func (q *ConfigRabbitMQArgument) changeConsumer(length int) error {
	q.consumers = make([]messageConsumer, length)
	return nil
}

func (q *ConfigRabbitMQArgument) logger() {
	q.log = logrus.New()
	q.log.SetFormatter(&logrus.JSONFormatter{})
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
		return err
	}

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
func (q *ConfigRabbitMQArgument) Close() error {
	if q.closed {
		return fmt.Errorf("error already closed")
	}

	if err := q.channel.Close(); err != nil {
		return err
	}

	if err := q.connection.Close(); err != nil {
		return err
	}

	q.closed = true
	return nil
}

func (q *ConfigRabbitMQArgument) reconnector() {
	for {
		err := <-q.errorChannel
		if !q.closed {
			q.log.Errorf("Reconnecting after connection closed, %v", err)

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

const timesleepMs = 1000

func (q *ConfigRabbitMQArgument) connect() {
	for {
		q.log.Infof("Connecting to rabbitmq on %s", q.url)
		conn, err := amqp.Dial(q.url)
		if err != nil {
			q.log.Errorf("Connection to rabbitmq failed. Retrying in 1 sec... ", err)
			time.Sleep(timesleepMs * time.Millisecond)
			return
		}

		q.connection = conn
		q.errorChannel = make(chan *amqp.Error)
		q.connection.NotifyClose(q.errorChannel)

		q.log.Info("Connection established!")

		q.openChannel()
		if q.Type == ClientConsumerType {
			q.declareExchangeQueue()
			q.declareQueue()
			q.bindingQueue()
		}
	}
}

func (q *ConfigRabbitMQArgument) declareExchangeQueue() error {
	err := q.channel.ExchangeDeclare(
		q.ExchangeQueue.Name,       // name of the exchange
		q.ExchangeQueue.Type,       // type
		q.ExchangeQueue.Durable,    // durable
		q.ExchangeQueue.AutoDelete, // delete when complete
		false,                      // internal
		false,                      // noWait
		nil,                        // arguments
	)

	if err != nil {
		return err
	}

	return nil
}

func (q *ConfigRabbitMQArgument) declareQueue() error {
	_, err := q.channel.QueueDeclare(
		q.QueueConfig.Name,       // name
		q.QueueConfig.Durable,    // durable
		q.QueueConfig.AutoDelete, // delete when unused
		false,                    // exclusive
		false,                    // no-wait
		nil,                      // arguments
	)

	if err != nil {
		return err
	}

	return nil
}

func (q *ConfigRabbitMQArgument) bindingQueue() error {
	err := q.channel.QueueBind(
		q.QueueBind.Name,       // name of the queue
		q.QueueBind.RoutingKey, // bindingKey
		q.ExchangeQueue.Name,   // sourceExchange
		false,                  // noWait
		nil,                    // arguments
	)

	if err != nil {
		return err
	}

	return nil
}

func (q *ConfigRabbitMQArgument) openChannel() error {
	channel, err := q.connection.Channel()
	if err != nil {
		q.log.Errorf("error opening channel failed, error:%v", err)
		return err
	}

	q.channel = channel
	return nil
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
		q.log.Infoln("Recovering consumer...")

		msgs, err := q.registerQueueConsumer()
		q.log.Infoln("Consumer recovered! Continuing message processing...")
		q.executeMessageConsumer(err, consumer, msgs, true)
	}
}

// AvailableMessageQueueType is availabel type
func AvailableMessageQueueType() []string {
	return []string{ClientConsumerType, ClientProducerType}
}

// MessageQueueTypeValid validate type
func MessageQueueTypeValid(ty string) bool {
	for _, mqType := range AvailableMessageQueueType() {
		if ty == mqType {
			return true
		}
	}

	return false
}
