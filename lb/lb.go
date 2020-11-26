package lb

import (
	"sync"
	"sync/atomic"
)

type LoadBalancerInterface interface {
	Get() (*Item, error)
	Remove(id int64)
	RemoveAll(func(int64, interface{}))
}

type Item struct {
	id       int64
	instance interface{}
}

func (i *Item) Instance() interface{} {
	return i.instance
}

type LoadBalancer struct {
	total   int64
	new     func(id int64) (interface{}, error)
	lists   []*Item
	current int64
	mu      sync.RWMutex
}

func (l *LoadBalancer) RemoveAll(fn func(id int64, instance interface{})) {
	l.mu.Lock()
	defer l.mu.Unlock()
	wg := sync.WaitGroup{}
	wg.Add(len(l.lists))
	for _, list := range l.lists {
		go func(item *Item) {
			defer wg.Done()
			fn(item.id, item.instance)
		}(list)
	}

	wg.Wait()
}

func NewLoadBalancer(total int64, new func(id int64) (interface{}, error)) *LoadBalancer {
	return &LoadBalancer{total: total, new: new, current: -1}
}

func (l *LoadBalancer) Get() (*Item, error) {
	var (
		err    error
		newOne interface{}
	)

	current := atomic.AddInt64(&l.current, 1)
	next := current % l.total

	l.mu.Lock()
	defer l.mu.Unlock()

	if int64(len(l.lists)-1) >= next {
		return l.lists[next], nil
	}

	if newOne, err = l.new(current); err != nil {
		return nil, err
	}

	l.lists = append(l.lists, &Item{
		id:       current,
		instance: newOne,
	})

	return l.lists[next], nil
}

func (l *LoadBalancer) Remove(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for index, list := range l.lists {
		if list.id == id {
			l.lists = append(l.lists[0:index], l.lists[index+1:]...)
			return
		}
	}
}
