package cache

import (
	"sync"
)

// DefaultBoundedCacheSize is the default max size when NewBoundedCache is called with maxSize <= 0.
const DefaultBoundedCacheSize = 100

type boundedEntry[V any] struct {
	value V
	slot  int
}

// BoundedCache is an in-memory FIFO cache with a fixed maximum size. Eviction follows insertion order.
// Deleted keys leave a ghost slot in the ring; ghosts are skipped on eviction so FIFO order is preserved.
type BoundedCache[K comparable, V any] struct {
	mu       sync.RWMutex
	keys     []K
	index    map[K]boundedEntry[V]
	head     int
	ringUsed int
	maxSize  int
}

// NewBoundedCache returns a BoundedCache with the given max size (or DefaultBoundedCacheSize if <= 0).
func NewBoundedCache[K comparable, V any](maxSize int) *BoundedCache[K, V] {
	if maxSize <= 0 {
		maxSize = DefaultBoundedCacheSize
	}
	return &BoundedCache[K, V]{
		keys:    make([]K, maxSize),
		index:   make(map[K]boundedEntry[V], maxSize),
		maxSize: maxSize,
	}
}

// Get returns the value for key and true if found.
func (c *BoundedCache[K, V]) Get(key K) (V, bool) {
	if c == nil {
		var zero V
		return zero, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.index[key]
	return e.value, ok
}

func (c *BoundedCache[K, V]) insert(key K, value V) {
	for c.ringUsed >= c.maxSize {
		evictedKey := c.keys[c.head]
		if e, ok := c.index[evictedKey]; ok && e.slot == c.head {
			delete(c.index, evictedKey)
		}
		c.head = (c.head + 1) % c.maxSize
		c.ringUsed--
	}
	slot := (c.head + c.ringUsed) % c.maxSize
	c.keys[slot] = key
	c.index[key] = boundedEntry[V]{value: value, slot: slot}
	c.ringUsed++
}

// Set inserts or updates the entry for key. If the key already exists, its value is updated in place;
// eviction order is unchanged (FIFO by first insertion).
func (c *BoundedCache[K, V]) Set(key K, value V) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.index[key]; ok {
		c.index[key] = boundedEntry[V]{value: value, slot: e.slot}
		return
	}
	c.insert(key, value)
}

// SetIfAbsent adds the entry for key only if it does not exist; does nothing if key is already present.
func (c *BoundedCache[K, V]) SetIfAbsent(key K, value V) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.index[key]; ok {
		return
	}
	c.insert(key, value)
}

// Delete removes the entry for key if present. No-op if key is not in the cache.
// The slot remains in the ring as a ghost and is skipped on eviction.
func (c *BoundedCache[K, V]) Delete(key K) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.index, key)
}

// Len returns the number of entries in the cache.
func (c *BoundedCache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.index)
}
