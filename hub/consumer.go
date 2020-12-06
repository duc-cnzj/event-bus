package hub

import (
	"context"
	"github.com/streadway/amqp"
)

type MqConfigInterface interface {
	WithConsumerAck(needAck bool)
	WithConsumerReBalance(needReBalance bool)

	WithExchangeDurable(durable bool)
	WithExchangeAutoDelete(autoDelete bool)

	WithQueueAutoDelete(autoDelete bool)
	WithQueueDurable(durable bool)
}

type ConsumerInterface interface {
	Consume(ctx context.Context) (MessageInterface, error)
	Ack(string) error
	Nack(string) error
	Delivery() chan amqp.Delivery
	GetId() int64

	GetConn() *amqp.Connection
	GetChannel() *amqp.Channel
	GetQueueName() string
	GetKind() string
	GetExchange() string
	AutoAck() bool
	Close()
	ChannelDone() chan *amqp.Error
	RemoveSelf()

	DontNeedReBalance() bool
}

type ConsumerBase struct {
	id        int64
	queueName string
	cm        ConsumerManagerInterface
	queue     amqp.Queue
	conn      *amqp.Connection
	channel   *amqp.Channel
	exchange  string

	queueAutoDelete bool
	queueDurable    bool

	exchangeDurable    bool
	exchangeAutoDelete bool

	autoAck           bool
	dontNeedReBalance bool

	delivery <-chan amqp.Delivery

	kind string
	hub  Interface

	closed atomicBool

	closeChan chan *amqp.Error
}

type Option func(cci MqConfigInterface)

func WithConsumerReBalance(needReBalance bool) Option {
	return func(cci MqConfigInterface) {
		cci.WithConsumerReBalance(needReBalance)
	}
}

func WithConsumerAck(needAck bool) Option {
	return func(cci MqConfigInterface) {
		cci.WithConsumerAck(needAck)
	}
}

func WithExchangeDurable(durable bool) Option {
	return func(cci MqConfigInterface) {
		cci.WithExchangeDurable(durable)
	}
}

func WithExchangeAutoDelete(autoDelete bool) Option {
	return func(cci MqConfigInterface) {
		cci.WithExchangeAutoDelete(autoDelete)
	}
}

func WithQueueAutoDelete(autoDelete bool) Option {
	return func(cci MqConfigInterface) {
		cci.WithQueueAutoDelete(autoDelete)
	}
}

func WithQueueDurable(durable bool) Option {
	return func(cci MqConfigInterface) {
		cci.WithQueueDurable(durable)
	}
}
