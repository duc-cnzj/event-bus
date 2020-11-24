package hub

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"sync"
)

type Closeable interface {
	Done() chan *amqp.Error
}

type ProducerManagerInterface interface {
	GetProducer(queueName, kind string) (ProducerInterface, error)
	RemoveProducer(p ProducerInterface)
	CloseAll()
	Count() int
	Print()
}

type ProducerManager struct {
	hub       Interface
	mu        sync.RWMutex
	producers sync.Map
}

func NewProducerManager(hub *Hub) *ProducerManager {
	return &ProducerManager{hub: hub}
}

func (pm *ProducerManager) GetProducer(queueName, kind string) (ProducerInterface, error) {
	var (
		ok       bool
		producer ProducerInterface
		err      error
	)
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := pm.getKey(queueName, kind)
	if producer, ok = pm.getProducerIfHas(key); ok {
		return producer, nil
	}

	switch kind {
	case amqp.ExchangeDirect:
		if producer, err = NewDirectProducer(queueName, pm.hub).Build(); err != nil {
			log.Error(err)
			return nil, err
		}
		pm.producers.Store(key, producer)

		return producer, nil
	default:
		panic("err")
	}
}

func (pm *ProducerManager) RemoveProducer(p ProducerInterface) {
	pm.producers.Delete(pm.getKey(p.GetQueueName(), p.GetKind()))
}

func (pm *ProducerManager) CloseAll() {
	wg := sync.WaitGroup{}

	pm.producers.Range(func(key, value interface{}) bool {
		wg.Add(1)
		p := value.(ProducerInterface)
		go func() {
			defer wg.Done()
			p.Close()
		}()
		return true
	})

	wg.Wait()
}

func (pm *ProducerManager) Count() int {
	count := 0
	pm.producers.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func (pm *ProducerManager) Print() {
	pm.producers.Range(func(key, value interface{}) bool {
		p := value.(ProducerInterface)
		log.Warnf("key %s queue %v %v", key, p.GetQueueName(), p.GetQueueName())
		return true
	})
}

func (pm *ProducerManager) getProducerIfHas(key string) (ProducerInterface, bool) {
	if load, ok := pm.producers.Load(key); ok {
		return load.(ProducerInterface), true
	}

	return nil, false
}

func (pm *ProducerManager) getKey(queueName, kind string) string {
	return getKey(queueName, kind)
}

type ConsumerManagerInterface interface {
	GetConsumer(queueName, kind string) (ConsumerInterface, error)
	RemoveConsumer(ConsumerInterface)
	CloseAll()
	Count() int
	Print()
}

type ConsumerManager struct {
	hub       Interface
	mu        sync.RWMutex
	consumers sync.Map
}

func NewConsumerManager(hub *Hub) *ConsumerManager {
	return &ConsumerManager{hub: hub}
}

func (cm *ConsumerManager) GetConsumer(queueName, kind string) (ConsumerInterface, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var (
		ok       bool
		err      error
		consumer ConsumerInterface
	)

	cm.mu.Lock()
	defer cm.mu.Unlock()
	key := cm.getKey(queueName, kind)

	if consumer, ok = cm.getConsumerIfHas(key); ok {
		return consumer, nil
	}

	switch kind {
	case amqp.ExchangeDirect:
		if consumer, err = NewDirectConsumer(queueName, cm.hub).Build(); err != nil {
			return nil, err
		}

		cm.consumers.Store(key, consumer)

		return consumer, nil
	default:
		panic("err")
	}
}

func (cm *ConsumerManager) RemoveConsumer(c ConsumerInterface) {
	cm.consumers.Delete(cm.getKey(c.GetQueueName(), c.GetKind()))
}

func (cm *ConsumerManager) CloseAll() {
	wg := sync.WaitGroup{}

	cm.consumers.Range(func(key, value interface{}) bool {
		wg.Add(1)
		c := value.(ConsumerInterface)
		go func() {
			defer wg.Done()
			c.Close()
		}()
		return true
	})

	wg.Wait()
}

func (cm *ConsumerManager) Count() int {
	count := 0
	cm.consumers.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func (cm *ConsumerManager) Print() {
	cm.consumers.Range(func(key, value interface{}) bool {
		c := value.(ConsumerInterface)
		log.Warnf("key %s queue %v %v", key, c.GetQueueName(), c.GetQueueName())
		return true
	})
}

func (cm *ConsumerManager) getKey(queueName, kind string) string {
	return getKey(queueName, kind)
}

func (cm *ConsumerManager) getConsumerIfHas(key string) (ConsumerInterface, bool) {
	if load, ok := cm.consumers.Load(key); ok {
		return load.(ConsumerInterface), true
	}

	return nil, false
}

func getKey(queueName, kind string) string {
	return queueName + "@" + kind
}
