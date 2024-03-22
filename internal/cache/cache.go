package cache

import (
	"errors"
	"sync"
	"time"
)

type entry[T any] struct {
	v   T
	ttl int64
}

type Cache[T any] struct {
	defExp time.Duration

	mu struct {
		sync.RWMutex

		data map[string]*entry[T]
	}
}

func New[T any](exp time.Duration) *Cache[T] {
	data := make(map[string]*entry[T])
	c := &Cache[T]{
		defExp: exp,
	}
	c.mu.data = data
	go c.runCleaner()
	return c
}

func (c *Cache[T]) Set(key string, value T) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.contains(key) {
		return errors.New("key already exists")
	}

	return c.internalSet(key, value)
}

func (c *Cache[T]) Get(key string) (T, error) {
	reset := func(e *entry[T], exp time.Duration) {
		e.ttl = getTtl(exp)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	e, found := c.mu.data[key]
	if !found {
		return *new(T), errors.New("key not exist")
	}

	reset(e, c.defExp)
	return e.v, nil
}

func (c *Cache[T]) contains(key string) bool {
	_, found := c.mu.data[key]
	return found
}

func (c *Cache[T]) internalSet(key string, value T) error {
	e := &entry[T]{
		v:   value,
		ttl: getTtl(c.defExp),
	}
	c.mu.data[key] = e
	return nil
}

func getTtl(exp time.Duration) int64 {
	return time.Now().Add(exp).UnixMicro()
}

func (c *Cache[T]) runCleaner() {
	ticker := time.NewTicker(c.defExp)
	for range ticker.C {
		c.clearExpired()
	}
}

func (c *Cache[T]) clearExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := time.Now().UnixMicro()

	for k, e := range c.mu.data {
		if e.ttl <= t {
			e = nil
			delete(c.mu.data, k)
		}
	}
}
