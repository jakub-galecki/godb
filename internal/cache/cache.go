package cache

import (
	"github.com/jakub-galecki/godb/common"
	"sync"
	"time"
)

const (
	defaultExp = 3 * time.Second
)

type Cacher[T any] interface {
	Set(key string, value T) error
	Get(key string) (T, error)
	Has(key string) bool
}

type entry[T any] struct {
	v   T
	ttl int64
}

type CacheOptionFunc[T any] func(*cache[T])

func WithVerbose[T any](v bool) CacheOptionFunc[T] {
	return func(c *cache[T]) {
		c.verbose = v
	}
}

func WithExpiration[T any](exp time.Duration) CacheOptionFunc[T] {
	return func(c *cache[T]) {
		c.defExp = exp
	}
}

type cache[T any] struct {
	defExp time.Duration

	mu struct {
		sync.RWMutex

		data map[string]*entry[T]
	}

	verbose bool
}

func New[T any](opts ...CacheOptionFunc[T]) Cacher[T] {
	data := make(map[string]*entry[T])
	c := &cache[T]{
		defExp: defaultExp,
	}

	for _, opt := range opts {
		opt(c)
	}

	c.mu.data = data
	go c.runCleaner()
	return c
}

func (c *cache[T]) Set(key string, value T) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.internalSet(key, value)
}

func (c *cache[T]) Get(key string) (T, error) {
	reset := func(e *entry[T], exp time.Duration) {
		e.ttl = getTtl(exp)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	e, found := c.mu.data[key]
	if !found {
		return *new(T), common.ErrKeyNotFound
	}

	reset(e, c.defExp)
	return e.v, nil
}

func (c *cache[T]) Has(key string) bool {
	_, found := c.mu.data[key]
	return found
}

func (c *cache[T]) internalSet(key string, value T) error {
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

func (c *cache[T]) runCleaner() {
	ticker := time.NewTicker(c.defExp)
	for range ticker.C {
		c.clearExpired()
	}
}

func (c *cache[T]) clearExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := time.Now().UnixMicro()

	if c.verbose {
		// trace.Debug().
		// 	Msg("cleaning expired cache entries")
	}
	for k, e := range c.mu.data {
		if e.ttl <= t {
			// trace.Debug().
			// 	Str("removing key", k)

			e = nil
			delete(c.mu.data, k)
		}
	}
}
