package hub

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"mq/lb"
	"sync"
)

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
		one *lb.Item
		err error
	)
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := pm.getKey(queueName, kind)

	if load, ok := pm.producers.Load(key); ok {
		if item, err := load.(lb.LoadBalancerInterface).Get(); err != nil {
			return nil, err
		} else {
			return item.Instance().(ProducerInterface), nil
		}
	}

	loadBalancer := lb.NewLoadBalancer(pm.hub.Config().EachQueueProducerNum, func(id int64) (interface{}, error) {
		switch kind {
		case amqp.ExchangeDirect:
			return NewDirectProducer(queueName, pm.hub, id).Build()
		default:
			return nil, errors.New("unsupport kind: " + kind)
		}
	})

	pm.producers.Store(key, loadBalancer)

	if one, err = loadBalancer.Get(); err != nil {
		return nil, err
	}

	return one.Instance().(ProducerInterface), nil

}

func (pm *ProducerManager) RemoveProducer(p ProducerInterface) {
	if load, ok := pm.producers.Load(pm.getKey(p.GetQueueName(), p.GetKind())); ok {
		load.(lb.LoadBalancerInterface).Remove(p.GetId())
	}
}

func (pm *ProducerManager) CloseAll() {
	log.Warn("start close all producers.")
	wg := sync.WaitGroup{}

	pm.producers.Range(func(key, value interface{}) bool {
		wg.Add(1)
		defer wg.Done()
		p := value.(lb.LoadBalancerInterface)
		go func() {
			p.RemoveAll(func(key int64, instance interface{}) {
				instance.(ProducerInterface).Close()
			})
		}()
		return true
	})

	wg.Wait()
	log.Warn("end close all producers.")
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
		one *lb.Item
		err error
	)

	key := cm.getKey(queueName, kind)

	if load, ok := cm.consumers.Load(key); ok {
		if one, err = load.(lb.LoadBalancerInterface).Get(); err != nil {
			return nil, err
		} else {
			return one.Instance().(ConsumerInterface), nil
		}
	}
	log.Error(cm.hub.Config().EachQueueConsumerNum)
	loadBalancer := lb.NewLoadBalancer(cm.hub.Config().EachQueueConsumerNum, func(id int64) (interface{}, error) {
		switch kind {
		case amqp.ExchangeDirect:
			log.Error("new consumer ", id)
			return NewDirectConsumer(queueName, cm.hub, id).Build()
		default:
			return nil, errors.New("unsupport kind: " + kind)
		}
	})

	cm.consumers.Store(key, loadBalancer)

	if one, err = loadBalancer.Get(); err != nil {
		return nil, err
	}

	return one.Instance().(ConsumerInterface), nil
}

func (cm *ConsumerManager) RemoveConsumer(c ConsumerInterface) {
	if load, ok := cm.consumers.Load(cm.getKey(c.GetQueueName(), c.GetKind())); ok {
		load.(lb.LoadBalancerInterface).Remove(c.GetId())
	}
}

func (cm *ConsumerManager) CloseAll() {
	log.Warn("start close all consumers.")

	wg := sync.WaitGroup{}

	cm.consumers.Range(func(key, value interface{}) bool {
		wg.Add(1)
		defer wg.Done()
		value.(lb.LoadBalancerInterface).RemoveAll(func(i int64, instance interface{}) {
			go func() {
				instance.(ConsumerInterface).Close()
			}()
		})
		return true
	})

	wg.Wait()
	log.Warn("end close all consumers.")

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
