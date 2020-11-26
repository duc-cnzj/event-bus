package lb

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestNewLoadBalancer(t *testing.T) {
	lb := NewLoadBalancer(10, func(id int64) (interface{}, error) {
		return "get " + strconv.Itoa(int(id)), nil
	})
	mu := sync.Mutex{}
	var result = map[interface{}]int{}

	wg := sync.WaitGroup{}
	num := 200000
	wg.Add(num)
	for i := 0; i < num; i++ {
		go func() {
			defer wg.Done()
			instance, _ := lb.Get()
			mu.Lock()
			result[instance]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	for _, n := range result {
		if n != num/10 {
			t.Errorf("error expect %d got %d", num/10, n)
		}
	}
}

func TestLoadBalancer_Remove(t *testing.T) {
	lb := NewLoadBalancer(10, func(id int64) (interface{}, error) {
		return "get " + strconv.Itoa(int(id)), nil
	})
	for i := 0; i < 10; i++ {
		lb.Get()
		if len(lb.lists) != i+1 {
			t.Errorf("lb.lists error want %d got %d", i+1, len(lb.lists))
		}
	}

	lb.Remove(0)
	lb.Remove(10)

	for i := 0; i < 1000; i++ {
		lb.Get()
	}

	for _, list := range lb.lists {
		fmt.Println(list)
	}
}

func BenchmarkNewLoadBalancer(b *testing.B) {
	lb := NewLoadBalancer(1000, func(id int64) (interface{}, error) {
		return "get " + strconv.Itoa(int(id)), nil
	})
	for i := 0; i < b.N; i++ {
		lb.Get()
	}
}
