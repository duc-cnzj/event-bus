package hub

import (
	"github.com/streadway/amqp"
)

type ProducerInterface interface {
	GetId() int64
	GetConn() *amqp.Connection
	GetChannel() *amqp.Channel
	GetQueueName() string
	GetKind() string
	GetExchange() string
	DelayPublish(string, Message, uint) error
	Publish(Message) error
	Close()
	ChannelDone() chan *amqp.Error
}

type ProducerBase struct {
	id        int64
	pm        ProducerManagerInterface
	queueName string
	queue     amqp.Queue
	conn      *amqp.Connection
	channel   *amqp.Channel
	exchange  string

	queueAutoDelete bool
	queueDurable    bool

	exchangeDurable    bool
	exchangeAutoDelete bool

	kind string
	hub  Interface

	closed atomicBool

	closeChan chan *amqp.Error
}
