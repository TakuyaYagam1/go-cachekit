package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/singleflight"
)

const defaultLoadTimeout = 30 * time.Second

// cachedValueConfig is the configuration for a CachedValue.
type cachedValueConfig struct {
	loadTimeout time.Duration
}

// CachedValueOption configures CachedValue at construction.
type CachedValueOption func(*cachedValueConfig)

// WithLoadTimeout sets the context timeout for the load function in Get. Zero means 30s default.
func WithLoadTimeout(d time.Duration) CachedValueOption {
	return func(c *cachedValueConfig) { c.loadTimeout = d }
}

// CachedValue caches a single value by key with TTL and singleflight.
// LoadTimeout limits the context timeout for the load function in Get; zero means 30s.
type CachedValue[T any] struct {
	c           *ttlcache.Cache[string, T]
	sf          singleflight.Group
	key         string
	ttl         time.Duration
	loadTimeout time.Duration
	done        chan struct{}
	once        sync.Once
	version     atomic.Uint64
	mu          sync.Mutex
}

// NewCachedValue returns a CachedValue for the given key and ttl. Panics if ttl <= 0.
// Use NewCachedValueE when ttl is config-driven or may be invalid to avoid panics (it returns an error instead of panicking). When ctx is cancelled, the internal goroutine is stopped; the caller may also call Stop explicitly. If using context.Background(), the caller must call Stop() when done to avoid goroutine leaks.
func NewCachedValue[T any](ctx context.Context, key string, ttl time.Duration, opts ...CachedValueOption) *CachedValue[T] {
	v, err := NewCachedValueE[T](ctx, key, ttl, opts...)
	if err != nil {
		panic(err.Error())
	}
	return v
}

// NewCachedValueE returns a CachedValue and an error if ttl is not positive.
func NewCachedValueE[T any](ctx context.Context, key string, ttl time.Duration, opts ...CachedValueOption) (*CachedValue[T], error) {
	if key == "" {
		return nil, fmt.Errorf("cache: NewCachedValue key must be non-empty")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("cache: NewCachedValue ttl must be positive, got %v", ttl)
	}
	cfg := &cachedValueConfig{loadTimeout: defaultLoadTimeout}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	loadTimeout := cfg.loadTimeout
	if loadTimeout <= 0 {
		loadTimeout = defaultLoadTimeout
	}
	c := ttlcache.New(
		ttlcache.WithTTL[string, T](ttl),
		ttlcache.WithCapacity[string, T](1),
	)
	v := &CachedValue[T]{c: c, key: key, ttl: ttl, loadTimeout: loadTimeout, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
		case <-v.done:
		}
		c.Stop()
	}()
	go c.Start()
	return v, nil
}

// Get returns the cached value or calls load, caches the result, and returns it.
func (v *CachedValue[T]) Get(ctx context.Context, load func(context.Context) (T, error)) (T, error) {
	var zero T
	if v == nil {
		return zero, fmt.Errorf("cache: CachedValue is nil")
	}
	if item := v.c.Get(v.key); item != nil {
		return item.Value(), nil
	}
	res, err, _ := v.sf.Do(v.key, func() (any, error) {
		if item := v.c.Get(v.key); item != nil {
			return item.Value(), nil
		}
		verBefore := v.version.Load()
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), v.loadTimeout)
		val, err := load(loadCtx)
		cancel()
		if err != nil {
			return zero, err
		}
		v.mu.Lock()
		if v.version.Load() == verBefore {
			v.c.Set(v.key, val, v.ttl)
		}
		v.mu.Unlock()
		return val, nil
	})
	if err != nil {
		return zero, err
	}
	typed, ok := res.(T)
	if !ok && res != nil {
		return zero, fmt.Errorf("cached value: unexpected type %T", res)
	}
	return typed, nil
}

// GetStale returns the cached value if present, without calling load.
func (v *CachedValue[T]) GetStale() (T, bool) {
	var zero T
	if v == nil {
		return zero, false
	}
	if item := v.c.Get(v.key); item != nil {
		return item.Value(), true
	}
	return zero, false
}

// Invalidate removes the cached value and forgets the singleflight key.
// An in-flight Get load that finishes after Invalidate will not write back to the cache.
func (v *CachedValue[T]) Invalidate() {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.version.Add(1)
	v.sf.Forget(v.key)
	v.c.Delete(v.key)
	v.mu.Unlock()
}

// Stop stops the internal TTL cache goroutine. Call when the CachedValue is no longer used.
// Safe to call multiple times.
func (v *CachedValue[T]) Stop() {
	if v == nil {
		return
	}
	v.once.Do(func() { close(v.done) })
}
