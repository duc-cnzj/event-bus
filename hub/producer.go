package hub

import "github.com/streadway/amqp"

type ProducerInterface interface {
	GetConn() *amqp.Connection
	GetChannel() *amqp.Channel
	GetQueueName() string
	GetKind() string
	Publish(Message) error
	Close()
}

type Prepareable interface {
	PrepareConn() error
	PrepareChannel() error
	PrepareExchange() error
	PrepareQueueDeclare() error
	PrepareQueueBind() error
}

type ProducerBuilder interface {
	Build() (ProducerInterface, error)
}

type ProducerBase struct {
	queueName string
	conn      *amqp.Connection
	channel   *amqp.Channel
	exchange  string

	kind string
	hub  Interface

	closeChan chan *amqp.Error
}

var DefaultExchange = "duc_exchange"
