# mq

Client connection rabbitmq

### Config

```
config := &ConfigRabbitMQArgument{
  Host : "localhost",
  Port : 5762,
  Username: "admin",
  Password :"admin",

  QueueConfig: mq.QueueConfig{
    Name :"Queue name",
    Durable: true,
    AutoDelete: false,
  },

  QueueBind: mq.QueueBind {
    Name: "Queue bind",
    Key: "Queue Key",
    RutingKey:"Routing_Key"
  }

  ExchangeQueue: fmq.ExchangeQueueConfig{
 	  Name: "Exchange queue",
 	  Type: "direct",
 	  Durable: true,
 	  AutoDelete: false,
	},
}

// Producer
  // initialize
	producer = fmq.NewQueue(config)

  // send data
  data := []byte{}
  producer.Send(data)

// consumer
  // Set FnCallback function

    config.FnCallback = Stream

  // Create new function
  // function for receive data
  func Stream(data []byte){
    log.Println(string(data))
  }


  // Call consumer
  func StreamData(){
    consumer.Consumer()
  }

```
