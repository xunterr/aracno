package inmem

import (
	"sync"

	"github.com/xunterr/aracno/internal/storage"
)

type InMemoryStorage[V any] struct {
	store   map[string]V
	storeMu sync.Mutex
}

func NewInMemoryStorage[V any]() *InMemoryStorage[V] {
	return &InMemoryStorage[V]{
		store: make(map[string]V),
	}
}

func (s *InMemoryStorage[V]) Get(key string) (V, error) {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()

	val, ok := s.store[key]
	if !ok {
		return *new(V), storage.NoSuchKeyError
	}

	return val, nil
}

func (s *InMemoryStorage[V]) Put(key string, value V) error {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	s.store[key] = value
	return nil
}

func (s *InMemoryStorage[V]) Delete(key string) error {
	s.storeMu.Lock()
	delete(s.store, key)
	s.storeMu.Unlock()
	return nil
}

type SlidingStorage[V any] struct {
	storage storage.Storage[V]
	cache   *LruCache[V]
}

func NewSlidingStorage[V any](storage storage.Storage[V], windowSize uint) *SlidingStorage[V] {
	cache := NewLruCache[V](windowSize)

	cache.SetOnEvict(func(k string, v V) {
		storage.Put(k, v)
	})

	return &SlidingStorage[V]{
		storage: storage,
		cache:   cache,
	}

}

func (s *SlidingStorage[V]) Get(key string) (V, error) {
	if val, err := s.cache.Get(key); err == nil {
		return val, nil
	} else {
		return s.storage.Get(key)
	}
}

func (s *SlidingStorage[V]) Put(key string, val V) error {
	return s.cache.Put(key, val)
}

func (s *SlidingStorage[V]) Delete(key string) error {
	s.cache.Delete(key)
	return s.storage.Delete(key)
}
