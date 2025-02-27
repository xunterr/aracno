package inmem

import (
	"testing"

	"github.com/xunterr/aracno/internal/storage"
)

func setup() *LruCache[string] {
	return NewLruCache[string](3)
}

func TestEvict(t *testing.T) {
	cache := setup()
	var evicted string
	cache.SetOnEvict(func(k, v string) {
		evicted = v
	})

	cache.Put("1", "1")
	cache.Put("2", "2")
	cache.Put("3", "3")
	cache.Put("4", "4")

	if evicted != "1" {
		t.Fatalf("Evicted doesn't match expected output. Have: %s, want: 1", evicted)
	}
}

func TestPutGet(t *testing.T) {
	cache := setup()
	cache.Put("1", "1")
	cache.Put("2", "1")
	cache.Put("1", "2")

	val, err := cache.Get("1")
	if err != nil {
		t.Fatal(err.Error())
	}

	if val != "2" {
		t.Fatalf("Unexpected value. Have: %s, want: 2", val)
	}
}

func TestDelete(t *testing.T) {
	cache := setup()
	cache.Put("1", "1")

	cache.Delete("1")

	_, err := cache.Get("1")
	if err != storage.NoSuchKeyError {
		t.Fatalf("Value was not deleted")
	}
}
