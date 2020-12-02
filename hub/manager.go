package hub

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"mq/lb"
	"sync"
	"sync/atomic"
)

type ProducerManagerInterface interface {
	GetProducer(queueName, kind, exchange string) (ProducerInterface, error)
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

func (pm *ProducerManager) GetProducer(queueName, kind, exchange string) (ProducerInterface, error) {
	var (
		item *lb.Item
		err  error
	)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := pm.getKey(queueName, kind, exchange)

	if load, ok := pm.producers.Load(key); ok {
		if item, err := load.(lb.LoadBalancerInterface).Get(); err != nil {
			return nil, err
		} else {
			return item.Instance().(ProducerInterface), nil
		}
	}

	loadBalancer := lb.NewLoadBalancer(pm.hub.Config().EachQueueProducerNum, func(id int64) (interface{}, error) {
		select {
		case <-pm.hub.Done():
			return nil, ErrorHubDone
		default:
			switch kind {
			case amqp.ExchangeDirect:
				return NewDirectProducer(queueName, exchange, pm.hub, id).Build()
			case amqp.ExchangeFanout:
				return NewPubProducer(exchange, pm.hub, id).Build()
			default:
				return nil, errors.New("unsupport kind: " + kind)
			}
		}
	})

	pm.producers.Store(key, loadBalancer)

	if item, err = loadBalancer.Get(); err != nil {
		return nil, err
	}

	return item.Instance().(ProducerInterface), nil

}

func (pm *ProducerManager) RemoveProducer(p ProducerInterface) {
	if load, ok := pm.producers.Load(pm.getKey(p.GetQueueName(), p.GetKind(), p.GetExchange())); ok {
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
	var count int64 = 0

	pm.producers.Range(func(key, value interface{}) bool {
		atomic.AddInt64(&count, int64(value.(lb.LoadBalancerInterface).Count()))
		return true
	})

	return int(atomic.LoadInt64(&count))
}

func (pm *ProducerManager) Print() {
	pm.producers.Range(func(key, value interface{}) bool {
		value.(lb.LoadBalancerInterface).Range(func(key int, item *lb.Item) {
			p := item.Instance().(ProducerInterface)
			log.Infof("key %d queue %s id %d", key, p.GetQueueName(), p.GetId())
		})
		return true
	})
}

func (pm *ProducerManager) getKey(queueName, kind, exchange string) string {
	return getKey(queueName, kind, exchange)
}

type ConsumerManagerInterface interface {
	GetConsumer(queueName, kind, exchange string) (ConsumerInterface, error)
	RemoveConsumer(ConsumerInterface)
	Delivery(queueName, kind, exchange string) chan amqp.Delivery
	CloseAll()
	Count() int
	Print()
}

type ConsumerManager struct {
	hub         Interface
	mu          sync.RWMutex
	consumers   sync.Map
	deliveryMap sync.Map
}

func NewConsumerManager(hub *Hub) *ConsumerManager {
	return &ConsumerManager{hub: hub}
}

func (cm *ConsumerManager) Delivery(queueName, kind, exchange string) chan amqp.Delivery {
	key := cm.getKey(queueName, kind, exchange)
	if load, ok := cm.deliveryMap.Load(key); ok {
		return load.(chan amqp.Delivery)
	}

	ch := make(chan amqp.Delivery, 1)
	cm.deliveryMap.Store(key, ch)

	return ch
}

func (cm *ConsumerManager) GetConsumer(queueName, kind, exchange string) (ConsumerInterface, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var (
		item *lb.Item
		err  error
	)

	key := cm.getKey(queueName, kind, exchange)

	if load, ok := cm.consumers.Load(key); ok {
		if item, err = load.(lb.LoadBalancerInterface).Get(); err != nil {
			return nil, err
		} else {
			return item.Instance().(ConsumerInterface), nil
		}
	}

	loadBalancer := lb.NewLoadBalancer(cm.hub.Config().EachQueueConsumerNum, func(id int64) (interface{}, error) {
		select {
		case <-cm.hub.Done():
			return nil, ErrorHubDone
		default:
			switch kind {
			case amqp.ExchangeDirect:
				return NewDirectConsumer(queueName, exchange, cm.hub, id).Build()
			case amqp.ExchangeFanout:
				return NewSubConsumer(queueName, exchange, cm.hub, id).Build()
			default:
				return nil, errors.New("unsupport kind: " + kind)
			}
		}
	})

	cm.consumers.Store(key, loadBalancer)

	if item, err = loadBalancer.Get(); err != nil {
		return nil, err
	}

	return item.Instance().(ConsumerInterface), nil
}

func (cm *ConsumerManager) RemoveConsumer(c ConsumerInterface) {
	if load, ok := cm.consumers.Load(cm.getKey(c.GetQueueName(), c.GetKind(), c.GetExchange())); ok {
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
	var count int64 = 0

	cm.consumers.Range(func(key, value interface{}) bool {
		atomic.AddInt64(&count, int64(value.(lb.LoadBalancerInterface).Count()))
		return true
	})

	return int(atomic.LoadInt64(&count))
}

func (cm *ConsumerManager) Print() {
	cm.consumers.Range(func(key, value interface{}) bool {
		value.(lb.LoadBalancerInterface).Range(func(key int, item *lb.Item) {
			c := item.Instance().(ConsumerInterface)
			log.Infof("key %d queue %s id %d.", key, c.GetQueueName(), c.GetId())
		})
		return true
	})
}

func (cm *ConsumerManager) getKey(queueName, kind, exchange string) string {
	return getKey(queueName, kind, exchange)
}

func getKey(queueName, kind, exchange string) string {
	return queueName + "@" + kind + "@" + exchange
}
