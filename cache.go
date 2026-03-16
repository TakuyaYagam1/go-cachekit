package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	defaultGetOrLoadTimeout     = 30 * time.Second
	defaultMaxVersionMapEntries = 65536
	evictCollectCap             = 4096
)

var inFlightDeleted = &atomic.Int64{}

// Cache provides Redis-backed JSON get/set with singleflight for GetOrLoad.
// Use each key with a single type T; mixing types for the same key causes errors.
// Del and DeleteByPrefix increment a per-key version; an in-flight load that finishes after Del will not write back.
// Key versions are stored in a sync.Map. When the map size exceeds max entries (default 65536), excess entries are evicted (no ordering guarantee). Keys currently in flight for GetOrLoad are never evicted. Set WithMaxVersionMapEntries(0) for no limit (not recommended for long-lived instances).
type Cache struct {
	redis                *redis.Client
	sf                   singleflight.Group
	versionMap           sync.Map
	versionMapSize       atomic.Int64
	inFlightKeys         sync.Map
	maxVersionMapEntries int
	evictMu              sync.Mutex
}

// CacheOption configures Cache at construction.
type CacheOption func(*Cache)

func escapeRedisGlob(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\', '*', '?', '[', ']':
			b.WriteRune('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// WithMaxVersionMapEntries limits the number of version map entries; when exceeded, excess entries are evicted (no ordering guarantee). Zero means no limit (use only for short-lived caches).
func WithMaxVersionMapEntries(n int) CacheOption {
	return func(c *Cache) {
		c.maxVersionMapEntries = n
	}
}

// New returns a Cache that uses the given Redis client. Optional CacheOptions configure the cache.
func New(redis *redis.Client, opts ...CacheOption) *Cache {
	c := &Cache{redis: redis, maxVersionMapEntries: defaultMaxVersionMapEntries}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

func addInFlight(c *Cache, key string) {
	for {
		newC := &atomic.Int64{}
		newC.Store(1)
		v, loaded := c.inFlightKeys.LoadOrStore(key, newC)
		if !loaded {
			return
		}
		if v == inFlightDeleted {
			newC2 := &atomic.Int64{}
			newC2.Store(1)
			if c.inFlightKeys.CompareAndSwap(key, inFlightDeleted, newC2) {
				return
			}
			continue
		}
		v.(*atomic.Int64).Add(1)
		return
	}
}

func removeInFlight(c *Cache, key string) {
	v, ok := c.inFlightKeys.Load(key)
	if !ok {
		return
	}
	ctr, ok := v.(*atomic.Int64)
	if !ok || ctr == inFlightDeleted {
		return
	}
	ctr.Add(-1)
	if ctr.Load() == 0 && c.inFlightKeys.CompareAndSwap(key, ctr, inFlightDeleted) {
		c.inFlightKeys.CompareAndDelete(key, inFlightDeleted)
	}
}

func cacheKeyVersion(c *Cache, key string) *atomic.Uint64 {
	if v, ok := c.versionMap.Load(key); ok {
		return v.(*atomic.Uint64)
	}
	newVer := &atomic.Uint64{}
	actual, loaded := c.versionMap.LoadOrStore(key, newVer)
	if !loaded {
		c.versionMapSize.Add(1)
		if c.maxVersionMapEntries != 0 && c.versionMapSize.Load() > int64(c.maxVersionMapEntries) {
			c.evictMu.Lock()
			evictVersionMapExcess(c, key)
			c.evictMu.Unlock()
		}
	}
	return actual.(*atomic.Uint64)
}

func evictVersionMapExcess(c *Cache, protectedKey string) {
	if c.maxVersionMapEntries == 0 {
		return
	}
	keys := make([]string, 0, evictCollectCap)
	c.versionMap.Range(func(k, _ any) bool {
		if len(keys) >= evictCollectCap {
			return false
		}
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
		return true
	})
	evict := int(c.versionMapSize.Load()) - c.maxVersionMapEntries
	if evict <= 0 {
		return
	}
	if evict > len(keys) {
		evict = len(keys)
	}
	deleted := 0
	for i := 0; i < len(keys) && deleted < evict; i++ {
		k := keys[i]
		if k == protectedKey {
			continue
		}
		if v, ok := c.inFlightKeys.Load(k); ok {
			if ptr, ok := v.(*atomic.Int64); ok && ptr != inFlightDeleted && ptr.Load() > 0 {
				continue
			}
		}
		c.versionMap.Delete(k)
		c.versionMapSize.Add(-1)
		deleted++
	}
}

// GetOrLoadOpts holds options for GetOrLoad. Use WithTimeout and WithRespectCallerCancel to configure.
type GetOrLoadOpts struct {
	Timeout             time.Duration
	RespectCallerCancel bool
}

// GetOrLoadOption configures GetOrLoad.
type GetOrLoadOption func(*GetOrLoadOpts)

// WithTimeout sets the load/set context timeout for GetOrLoad (default 30s). Must be positive.
func WithTimeout(d time.Duration) GetOrLoadOption {
	return func(o *GetOrLoadOpts) {
		if d > 0 {
			o.Timeout = d
		}
	}
}

// WithRespectCallerCancel makes loadFn see the caller's context cancellation; by default loadFn runs with context.WithoutCancel so it can finish and write back.
func WithRespectCallerCancel(respect bool) GetOrLoadOption {
	return func(o *GetOrLoadOpts) { o.RespectCallerCancel = respect }
}

// GetOrLoad returns the cached value for key or calls loadFn, stores the result with ttl, and returns it.
// Key must be non-empty. Use the same key only with one type T; otherwise concurrent calls with different T may get a type error.
// Del and DeleteByPrefix increment the key version so an in-flight load that completes after delete will not overwrite.
// There is a small TOCTOU window between the version check and Redis Set—a concurrent Del in that window may be overwritten by a stale Set; consistency is best-effort and TTL limits staleness.
// loadFn receives the request context and may respect context cancellation.
// If loadFn succeeds but Redis Set fails, returns (loadedData, nil): caller receives the data but the cache is not updated.
// Optional opts: WithTimeout, WithRespectCallerCancel.
func GetOrLoad[T any](c *Cache, ctx context.Context, key string, ttl time.Duration, loadFn func(context.Context) (T, error), opts ...GetOrLoadOption) (T, error) {
	var result T
	if c == nil || c.redis == nil {
		return result, ErrRedisNotConfigured
	}
	if key == "" {
		return result, fmt.Errorf("cache GetOrLoad: key must be non-empty")
	}
	if ttl <= 0 {
		return result, fmt.Errorf("cache GetOrLoad: ttl must be positive, got %v", ttl)
	}
	o := &GetOrLoadOpts{Timeout: defaultGetOrLoadTimeout}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = defaultGetOrLoadTimeout
	}
	respectCallerCancel := o.RespectCallerCancel
	val, err := c.redis.Get(ctx, key).Result()
	if err == nil {
		if unmarshalErr := json.Unmarshal([]byte(val), &result); unmarshalErr != nil {
			c.sf.Forget(key)
			cacheKeyVersion(c, key).Add(1)
			delErr := c.redis.Del(ctx, key).Err()
			var zero T
			if delErr != nil {
				return zero, fmt.Errorf("cache get unmarshal: %w (del failed: %v)", unmarshalErr, delErr)
			}
			return zero, fmt.Errorf("cache get unmarshal: %w", unmarshalErr)
		}
		return result, nil
	}
	if !errors.Is(err, redis.Nil) {
		var zero T
		return zero, fmt.Errorf("cache get: %w", err)
	}
	addInFlight(c, key)
	defer removeInFlight(c, key)
	ver := cacheKeyVersion(c, key)
	baseCtx := ctx
	if !respectCallerCancel {
		baseCtx = context.WithoutCancel(ctx)
	}
	v, err, _ := c.sf.Do(key, func() (any, error) {
		verBefore := ver.Load()
		loadCtx, cancel := context.WithTimeout(baseCtx, timeout)
		data, err := loadFn(loadCtx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("cache load: %w", err)
		}
		if ver.Load() != verBefore {
			return data, nil
		}
		bytes, marshalErr := json.Marshal(data)
		if marshalErr != nil {
			return nil, fmt.Errorf("cache load marshal: %w", marshalErr)
		}
		setCtx, cancel := context.WithTimeout(baseCtx, timeout)
		setErr := c.redis.Set(setCtx, key, bytes, ttl).Err()
		cancel()
		if setErr != nil {
			return data, nil
		}
		return data, nil
	})

	cached, ok := v.(T)
	if !ok && v != nil {
		var zero T
		return zero, fmt.Errorf("cache: unexpected type (possible type collision: same key used with different types)")
	}
	if err != nil {
		var zero T
		if ok {
			return cached, err
		}
		return zero, err
	}
	return cached, nil
}

// Del deletes keys and forgets them in singleflight. All keys must be non-empty.
func (c *Cache) Del(ctx context.Context, keys ...string) error {
	if c == nil || c.redis == nil {
		return ErrRedisNotConfigured
	}
	if len(keys) == 0 {
		return nil
	}
	for _, key := range keys {
		if key == "" {
			return fmt.Errorf("cache del: key must be non-empty")
		}
	}
	for _, key := range keys {
		c.sf.Forget(key)
		cacheKeyVersion(c, key).Add(1)
	}
	return c.redis.Del(ctx, keys...).Err()
}

// Set marshals value as JSON and stores it in Redis with the given ttl. Key must be non-empty; ttl must be positive.
func (c *Cache) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if c == nil || c.redis == nil {
		return ErrRedisNotConfigured
	}
	if key == "" {
		return fmt.Errorf("cache set: key must be non-empty")
	}
	if ttl <= 0 {
		return fmt.Errorf("cache set: ttl must be positive, got %v", ttl)
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("cache set marshal: %w", err)
	}
	if err := c.redis.Set(ctx, key, bytes, ttl).Err(); err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	c.sf.Forget(key)
	cacheKeyVersion(c, key).Add(1)
	return nil
}

const deleteByPrefixBatchSize = 500

// DeleteByPrefix scans keys matching prefix* and Unlinks them, and forgets them in singleflight.
// maxIterations is optional (variadic): pass one positive int to cap SCAN iterations and avoid blocking; 0 or omitted means no limit.
func (c *Cache) DeleteByPrefix(ctx context.Context, prefix string, maxIterations ...int) error {
	if c == nil || c.redis == nil {
		return ErrRedisNotConfigured
	}
	if prefix == "" {
		return fmt.Errorf("cache delete by prefix: empty prefix not allowed")
	}
	var limit int
	if len(maxIterations) > 0 {
		limit = maxIterations[0]
	}
	match := escapeRedisGlob(prefix) + "*"
	var cursor uint64
	iterations := 0
	for {
		if limit > 0 && iterations >= limit {
			break
		}
		keys, nextCursor, err := c.redis.Scan(ctx, cursor, match, deleteByPrefixBatchSize).Result()
		if err != nil {
			return fmt.Errorf("cache delete by prefix scan: %w", err)
		}
		if len(keys) > 0 {
			for _, key := range keys {
				c.sf.Forget(key)
				if v, ok := c.versionMap.Load(key); ok {
					v.(*atomic.Uint64).Add(1)
				}
			}
			if err := c.redis.Unlink(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("cache delete by prefix unlink: %w", err)
			}
		}
		iterations++
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}
