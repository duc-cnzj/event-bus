package hub

import (
	"context"
	"github.com/streadway/amqp"
)

type ConsumerInterface interface {
	GetConn() *amqp.Connection
	GetChannel() *amqp.Channel
	GetQueueName() string
	GetKind() string
	Consume(ctx context.Context) (string, uint64, error)
	Ack(uint64) error
	Nack(uint64) error
	Close()
}

type ConsumerBuilder interface {
	Build() (ConsumerInterface, error)
}

type PrepareConsumer interface {
	Prepareable
	PrepareQos() error
	PrepareDelivery() error
}

type ConsumerBase struct {
	queueName string
	conn      *amqp.Connection
	channel   *amqp.Channel
	exchange  string

	delivery <-chan amqp.Delivery

	kind string
	hub  Interface

	closeChan chan *amqp.Error
}
