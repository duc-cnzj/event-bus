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

type ConsumerBase struct {
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
