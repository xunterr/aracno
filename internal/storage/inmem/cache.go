package inmem

import "github.com/xunterr/aracno/internal/storage"

type LruCache[V any] struct {
	cache      map[string]V
	windowSize uint
	queue      InMemoryQueue[string]

	onEvict func(k string, v V)
}

func NewLruCache[V any](windowSize uint) *LruCache[V] {
	return &LruCache[V]{
		cache:      make(map[string]V, windowSize),
		windowSize: windowSize,
		queue:      InMemoryQueue[string]{},
		onEvict:    func(k string, v V) {},
	}
}

func (c *LruCache[V]) SetOnEvict(onEvictFunc func(k string, v V)) {
	c.onEvict = onEvictFunc
}

func (c *LruCache[V]) Get(key string) (V, error) {
	if val, ok := c.cache[key]; ok {
		return val, nil
	} else {
		return *new(V), storage.NoSuchKeyError
	}
}

func (c *LruCache[V]) Put(key string, val V) error {
	if len(c.cache) >= int(c.windowSize) {
		if err := c.offload(); err != nil {
			return err
		}
	}

	c.queue.Push(key)
	c.cache[key] = val
	return nil
}

func (c *LruCache[V]) offload() error {
	oldKey, err := c.queue.Pop()
	if err != nil {
		return err
	}

	if oldVal, ok := c.cache[oldKey]; ok {
		c.onEvict(oldKey, oldVal)
		delete(c.cache, oldKey)
	}
	return nil
}

func (c *LruCache[V]) Delete(key string) error {
	if _, ok := c.cache[key]; ok {
		delete(c.cache, key)
		return nil
	} else {
		return storage.NoSuchKeyError
	}
}
