package hub

import (
	"context"
	"github.com/streadway/amqp"
)

type ConsumerInterface interface {
	GetId() int64
	GetConn() *amqp.Connection
	GetChannel() *amqp.Channel
	GetQueueName() string
	GetKind() string
	Consume(ctx context.Context) (*Message, error)
	Ack(string) error
	Nack(string) error
	Close()
	Done() chan *amqp.Error
	Delivery() <-chan amqp.Delivery
}

type ConsumerBase struct {
	id        int64
	queueName string
	cm        ConsumerManagerInterface
	queue     amqp.Queue
	conn      *amqp.Connection
	channel   *amqp.Channel
	exchange  string

	delivery <-chan amqp.Delivery

	kind string
	hub  Interface

	closed atomicBool

	closeChan chan *amqp.Error
}
