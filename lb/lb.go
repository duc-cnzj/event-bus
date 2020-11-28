package lb

import (
	"sync"
	"sync/atomic"
)

var _ LoadBalancerInterface = (*LoadBalancer)(nil)

type LoadBalancerInterface interface {
	Get() (*Item, error)
	Count() int
	Remove(id int64)
	RemoveAll(func(int64, interface{}))
	Range(fn func(key int, item *Item))
}

type Item struct {
	id       int64
	instance interface{}
}

func (i *Item) Instance() interface{} {
	return i.instance
}

type LoadBalancer struct {
	removeList []int64
	total      int
	new        func(id int64) (interface{}, error)
	lists      []*Item
	current    int64
	mu         sync.RWMutex
}

func (l *LoadBalancer) Range(fn func(key int, item *Item)) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for key, list := range l.lists {
		fn(key, list)
	}
}

func (l *LoadBalancer) Count() int {
	return len(l.lists)
}

func (l *LoadBalancer) RemoveAll(fn func(id int64, instance interface{})) {
	l.mu.Lock()
	defer l.mu.Unlock()
	atomic.StoreInt64(&l.current, -1)

	wg := sync.WaitGroup{}
	wg.Add(len(l.lists))
	for _, list := range l.lists {
		go func(item *Item) {
			defer wg.Done()
			fn(item.id, item.instance)
		}(list)
	}

	wg.Wait()

	l.lists = nil
	l.initRemoveList()
}

func NewLoadBalancer(total int, new func(id int64) (interface{}, error)) *LoadBalancer {
	lb := &LoadBalancer{total: total, new: new, current: -1}

	lb.initRemoveList()

	return lb
}

func (l *LoadBalancer) initRemoveList() {
	for i := 0; i < l.total; i++ {
		l.removeList = append(l.removeList, int64(i))
	}
}

func (l *LoadBalancer) Get() (*Item, error) {
	var (
		err    error
		newOne interface{}
	)
	l.mu.Lock()
	defer l.mu.Unlock()

	current := atomic.AddInt64(&l.current, 1)
	next := current % int64(l.total)
	atomic.StoreInt64(&l.current, next)

	if int64(len(l.lists)-1) >= next {
		return l.lists[next], nil
	}

	if newOne, err = l.new(current); err != nil {
		atomic.AddInt64(&l.current, -1)

		return nil, err
	}

	l.lists = append(l.lists, &Item{
		id:       l.getId(),
		instance: newOne,
	})

	return l.lists[next], nil
}

func (l *LoadBalancer) getId() int64 {
	if len(l.removeList) > 0 {
		id := l.removeList[0]
		l.removeList = l.removeList[1:]

		return id
	}

	panic("err")
}

func (l *LoadBalancer) Remove(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for index, list := range l.lists {
		if list.id == id {
			l.removeList = append(l.removeList, id)
			l.lists = append(l.lists[0:index], l.lists[index+1:]...)
			if int(l.current)%l.total == index && l.current > -1 {
				atomic.AddInt64(&l.current, -1)
			}
			return
		}
	}
}
