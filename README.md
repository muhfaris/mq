# Message RabbitMQ Client

### Config
Valid type config below, it is case-sensitive.
- PRODUCER
- CONSUMER

#### Producer
```
// config producer
config := fmq.ConfigRabbitMQArgument{
	Host:     viper.GetString("publisher_message_queuing_service.host"),
	Port:     viper.GetInt("publisher_message_queuing_service.port"),
	Username: viper.GetString("publisher_message_queuing_service.username"),
	Password: viper.GetString("publisher_message_queuing_service.password"),
	Vhost:    viper.GetString("publisher_message_queuing_service.vhost"),

	QueueConfig: fmq.QueueConfig{
		Name:       viper.GetString("publisher_message_queuing_service.send_channel"),
		Durable:    true,
		AutoDelete: false,
	},

	QueueBind: fmq.QueueBindConfig{
		Name:       viper.GetString("publisher_message_queuing_service.send_channel"),
		RoutingKey: viper.GetString("publisher_message_queuing_service.send_channel"),
	},

	ExchangeQueue: fmq.ExchangeQueueConfig{
		Name:       viper.GetString("publisher_message_queuing_service.send_channel"),
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
	},
	Type: "PRODUCER",
}

// initialize
producer = fmq.NewQueue(config)

// send data
data := []byte("Hello world")
_ := producer.Send(data)

```

### Consumer
```
// config consumer
config:= fmq.ConfigRabbitMQArgument{
	Host:     viper.GetString("consumer_message_queuing_service.host"),
	Port:     viper.GetInt("consumer_message_queuing_service.port"),
	Username: viper.GetString("consumer_message_queuing_service.username"),
	Password: viper.GetString("consumer_message_queuing_service.password"),
	Vhost:    viper.GetString("consumer_message_queuing_service.vhost"),

	QueueConfig: fmq.QueueConfig{
		Name:       viper.GetString("consumer_message_queuing_service.read_channel"),
		Durable:    true,
		AutoDelete: false,
	},

	QueueBind: fmq.QueueBindConfig{
		Name:       viper.GetString("consumer_message_queuing_service.read_channel"),
		RoutingKey: viper.GetString("consumer_message_queuing_service.read_channel"),
	},

	QueueConsumer: fmq.QueueConsumer{
		QueueName: viper.GetString("consumer_message_queuing_service.read_channel"),
		AutoACK:   true,
	},

	ExchangeQueue: fmq.ExchangeQueueConfig{
		Name:       viper.GetString("consumer_message_queuing_service.read_channel"),
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
	},
	FnCallback: consumerfn,
	Type:       "CONSUMER",
}

// initialize
consumer = fmq.NewQueue(config)

// stream data
consumer.Consumer()
```

##### Create Consumer Function
```
//consumer.go
func Consumer(message []byte) {
    // do something here
    log.Println(string(message))
}

```
