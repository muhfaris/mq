package mq

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("session is shutting down")
)

// ConfigRabbitMQArgument wrap config rabbitmq
type ConfigRabbitMQArgument struct {
	Name          string
	Schema        string
	Host          string
	Port          int
	Username      string
	Password      string
	Vhost         string
	AutoReconnect bool
	Type          string

	PublisherConfig PublisherConfig
	QueueConfig     QueueConfig
	QueueBind       QueueBindConfig
	QueueConsumer   QueueConsumer
	ExchangeQueue   ExchangeQueueConfig
	QosConfig       QosConfig
}

// RabbitMQ is wrap rabbitMQ
type RabbitMQ struct {
	name            string
	schema          string
	url             string // generate
	connection      *amqp.Connection
	channel         *amqp.Channel
	closed          chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
	isConsumer      bool

	// publisher only
	PublisherConfig PublisherConfig

	// general
	QueueConfig   QueueConfig
	QueueBind     QueueBindConfig
	QueueConsumer QueueConsumer
	ExchangeQueue ExchangeQueueConfig
	QosConfig     QosConfig
	FnCallback    func([]byte)

	log *logrus.Logger
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
	AutoACK bool
}

type PublisherConfig struct {
	ContentType  string
	DeliveryMode uint8
}

type QosConfig struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

// NewQueue is Connection to rabbit
func NewQueue(cfg ConfigRabbitMQArgument) (*RabbitMQ, error) {
	url := cfg.getURL()

	if !MessageQueueTypeValid(cfg.Type) {
		return &RabbitMQ{}, fmt.Errorf("invalid type (CONSUMER / PRODUCER) of configuration")
	}

	if err := SchemaValid(cfg.Schema); err != nil {
		return &RabbitMQ{}, err
	}

	session := &RabbitMQ{
		name:          cfg.Name,
		schema:        cfg.Schema,
		url:           url,
		closed:        make(chan bool),
		log:           setupLogger(),
		QueueConfig:   cfg.QueueConfig,
		QueueBind:     cfg.QueueBind,
		QueueConsumer: cfg.QueueConsumer,
		ExchangeQueue: cfg.ExchangeQueue,
		QosConfig:     cfg.QosConfig,
		isConsumer:    isConsumer(cfg.Type),
	}

	go session.handleReconnect()
	return session, nil
}

func (q *ConfigRabbitMQArgument) getURL() string {
	return amqp.URI{
		Scheme:   q.Schema,
		Host:     q.Host,
		Port:     q.Port,
		Username: q.Username,
		Password: q.Password,
		Vhost:    q.Vhost,
	}.String()
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *RabbitMQ) handleReconnect() {
	for {
		session.isReady = false
		session.log.Infoln("Attempting to connect")

		conn, err := session.connect(session.url)
		if err != nil {
			session.log.Errorf("Failed to connect (%v). Retrying...", err)

			select {
			case <-session.closed:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (session *RabbitMQ) handleReInit(conn *amqp.Connection) bool {
	for {
		session.isReady = false

		err := session.init(conn)
		if err != nil {
			log.Println("Failed to initialize channel. Retrying...")

			select {
			case <-session.closed:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-session.closed:
			return true
		case <-session.notifyConnClose:
			session.log.Warn("Connection closed. Reconnecting...")
			return false
		case <-session.notifyChanClose:
			session.log.Warn("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel & declare queue
func (session *RabbitMQ) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(
		session.QueueConfig.Name,       // name
		session.QueueConfig.Durable,    // durable
		session.QueueConfig.AutoDelete, // delete when unused
		false,                          // exclusive
		false,                          // no-wait
		nil,                            // arguments
	)
	if err != nil {
		return fmt.Errorf("error queue declare: %v", err)
	}

	if session.isConsumer {
		err = ch.Qos(
			session.QosConfig.PrefetchCount, // prefetch count
			session.QosConfig.PrefetchSize,  // prefetch size
			session.QosConfig.Global,        // global
		)
		if err != nil {
			return fmt.Errorf("error qos: %v", err)
		}

		err := ch.ExchangeDeclare(
			session.ExchangeQueue.Name,    // name of the exchange
			session.ExchangeQueue.Type,    // type
			session.ExchangeQueue.Durable, // durable
			false,                         // delete when complete
			false,                         // internal
			false,                         // noWait
			nil,                           // arguments
		)
		if err != nil {
			return fmt.Errorf("error exchange declare: %v", err)
		}

		err = ch.QueueBind(
			session.QueueBind.Name,       // name of the queue
			session.QueueBind.RoutingKey, // bindingKey
			session.ExchangeQueue.Name,   // sourceExchange
			false,                        // noWait
			nil,                          // arguments
		)
		if err != nil {
			return fmt.Errorf("error queue bind: %v", err)
		}
	}

	session.changeChannel(ch)
	session.isReady = true
	session.log.Println("Setup!")

	return nil
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *RabbitMQ) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChanClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

// connect will create a new AMQP connection
func (session *RabbitMQ) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	session.log.Infoln("Connected!")
	return conn, nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *RabbitMQ) changeConnection(connection *amqp.Connection) {
	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error) // initialize
	session.connection.NotifyClose(session.notifyConnClose)
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (session *RabbitMQ) Push(data []byte) error {
	if !session.isReady {
		return errors.New("failed to push push: not connected")
	}
	for {
		err := session.UnsafePush(data)
		if err != nil {
			session.log.Warn("Push failed. Retrying...")
			select {
			case <-session.closed:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}

		select {
		case confirm := <-session.notifyConfirm:
			if confirm.Ack {
				session.log.Infoln("Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		session.log.Warn("Push didn't confirm. Retrying...")
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// recieve the message.
func (session *RabbitMQ) UnsafePush(data []byte) error {
	if !session.isReady {
		return errors.New("not connected")
	}

	return session.channel.Publish(
		session.ExchangeQueue.Name,   // Exchange
		session.QueueBind.RoutingKey, // Routing key
		false,                        // Mandatory
		false,                        // Immediate
		amqp.Publishing{
			DeliveryMode: session.PublisherConfig.DeliveryMode,
			ContentType:  session.PublisherConfig.ContentType,
			Body:         data,
		},
	)
}

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (session *RabbitMQ) Stream() (<-chan amqp.Delivery, error) {
	if session.isConnected() {
		session.log.Infoln("waiting for message...")
		return session.channel.Consume(
			session.QueueConfig.Name,
			"",                            // Consumer
			session.QueueConsumer.AutoACK, // Auto-Ack
			false,                         // Exclusive
			false,                         // No-local
			false,                         // No-Wait
			nil,                           // Args
		)

	}

	return (<-chan amqp.Delivery)(nil), nil
}

func (session *RabbitMQ) isConnected() bool {
	if !session.isReady {
		for {
			session.log.Warn("waiting for connection...")
			time.Sleep(5 * time.Second)
			return session.isConnected()
		}
	}

	return true
}

// Close will cleanly shutdown the channel and connection.
func (session *RabbitMQ) Close() error {
	if !session.isReady {
		return errAlreadyClosed
	}
	err := session.channel.Close()
	if err != nil {
		return err
	}
	err = session.connection.Close()
	if err != nil {
		return err
	}

	session.closed <- true
	session.isReady = false
	return nil
}

// Shutdown closes the RabbitMQ connection
func (session *RabbitMQ) Shutdown() error {
	return shutdown(session.connection)
}

// RegisterSignalHandler watchs for interrupt signals
// and gracefully closes connection
func (session *RabbitMQ) RegisterSignalHandler() {
	registerSignalHandler(session)
}

func isConsumer(messageType string) bool {
	return messageType == ClientConsumerType
}

func setupLogger() *logrus.Logger {
	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})
	return log
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

// SchemaValid is validate schema connection
func SchemaValid(schema string) error {
	switch schema {
	case "amqps":
		return nil
	case "amqp":
		return nil
	default:
		return errors.New("error schema is not valid (amqps / amqp)")
	}
}

// Closer interface is for handling reconnection logic in a sane way
// Every reconnection supported struct should implement those methods
// in order to work properly
type Closer interface {
	RegisterSignalHandler()
	Shutdown() error
}

// shutdown is a general closer function for handling close gracefully
// Mostly here for both consumers and producers
// After a reconnection scenerio we are gonna call shutdown before connection
func shutdown(conn *amqp.Connection) error {
	if err := conn.Close(); err != nil {
		if amqpError, isAmqpError := err.(*amqp.Error); isAmqpError && amqpError.Code != 504 {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	return nil
}

// shutdownChannel is a general closer function for channels
func shutdownChannel(channel *amqp.Channel, tag string) error {
	// This waits for a server acknowledgment which means the sockets will have
	// flushed all outbound publishings prior to returning.  It's important to
	// block on Close to not lose any publishings.
	if err := channel.Cancel(tag, true); err != nil {
		if amqpError, isAmqpError := err.(*amqp.Error); isAmqpError && amqpError.Code != 504 {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	if err := channel.Close(); err != nil {
		return err
	}

	return nil
}

// registerSignalHandler helper function for stopping consumer or producer from
// operating further
// Watchs for SIGINT, SIGTERM, SIGQUIT, SIGSTOP and closes connection
func registerSignalHandler(c Closer) {
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals)
		for {
			signal := <-signals
			switch signal {
			case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGSTOP:
				err := c.Shutdown()
				if err != nil {
					panic(err)
				}
				os.Exit(1)
			}
		}
	}()
}
