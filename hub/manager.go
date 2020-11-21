package hub

import (
	"github.com/streadway/amqp"
)

type Closeable interface {
	Done() chan *amqp.Error
}

type ProducerManagerInterface interface {
	GetProducer(queueName, kind string) (ProducerInterface, error)
}

type ProducerManager struct {
	hub Interface
}

func NewProducerManager(hub *Hub) *ProducerManager {
	return &ProducerManager{hub: hub}
}

func (pm *ProducerManager) GetProducer(queueName, kind string) (ProducerInterface, error) {
	switch kind {
	case amqp.ExchangeDirect:
		return NewDirectBuilder(queueName, pm.hub).Build()
	default:
		panic("err")
	}
}

type ConsumerManagerInterface interface {
	GetConsumer(queueName, kind string) (ConsumerInterface, error)
}

type ConsumerManager struct {
	hub Interface
}

func NewConsumerManager(hub *Hub) *ConsumerManager {
	return &ConsumerManager{hub: hub}
}

func (cm *ConsumerManager) GetConsumer(queueName, kind string) (ConsumerInterface, error) {
	switch kind {
	case amqp.ExchangeDirect:
		return NewDirectConsumer(queueName, cm.hub).Build()
	default:
		panic("err")
	}
}
