// Package cache provides Redis-backed and in-memory caching and key-value stores.
//
// # Redis connection
//
// NewRedisClient builds a redis-go client from RedisConfig and verifies connectivity with Ping.
// Host and Port are required; PoolSize and MinIdleConns default to 50 and 10 when zero.
// Do not log RedisConfig as-is; use GoString() to avoid exposing Password.
//
// # Redis JSON cache (Cache)
//
// New returns a Cache that uses the given *redis.Client. Optional CacheOptions (e.g. WithMaxVersionMapEntries) configure the cache. Values are serialized as JSON. Cache keys must be non-empty.
//
//   - GetOrLoad[T]: returns the value for key from Redis, or runs loadFn(ctx), stores the result with ttl, and returns it.
//     loadFn receives the request context and may respect context cancellation. Uses singleflight so concurrent
//     requests for the same key run loadFn once. Use each key with exactly one type T; using the same key with
//     different T causes a type error. ttl must be positive.
//   - Set: writes value as JSON with the given ttl (must be positive). Also forgets the key in singleflight and increments the per-key version so concurrent GetOrLoad does not use a stale singleflight result.
//   - Del: deletes keys and forgets them in singleflight.
//   - DeleteByPrefix: scans keys matching prefix*, Unlinks them, and forgets them in singleflight. prefix must be non-empty.
//     Redis glob characters (\, *, ?, [, ]) in prefix are escaped.
//
// GetOrLoad options: pass WithTimeout(d) and/or WithRespectCallerCancel(true) as variadic opts.
// Optional WithMaxVersionMapEntries(n) limits in-memory version map size; when exceeded, excess entries are evicted (no ordering guarantee).
// GetOrLoad caveats: Del and DeleteByPrefix increment a per-key version; a load that completes after the key was
// deleted will not write back to Redis. If loadFn succeeds but Redis Set fails, GetOrLoad returns (data, err)—caller
// gets the data but the cache is not updated (best-effort). Consistency is best-effort: there is a small race window
// between the in-memory version check and Redis Set; if Del runs in that window, a stale value may be written back.
// Values have TTL and will expire.
//
// # In-memory bounded cache (BoundedCache)
//
// NewBoundedCache[K, V](maxSize) creates a FIFO cache with at most maxSize entries (DefaultBoundedCacheSize when maxSize <= 0).
// Eviction is O(1) per slot via a ring buffer with lazy delete: Delete removes the key but leaves a ghost slot in the ring;
// ghosts are cleared on eviction so FIFO order is preserved. After many Deletes without Set, the next Set may take O(maxSize) steps to clear ghost slots. Set updates existing keys in place without changing eviction order. SetIfAbsent adds the entry only if the key is not present. Safe for concurrent use.
//
// # Single-value TTL cache (CachedValue)
//
// CachedValue caches one value by key with TTL and singleflight. Prefer NewCachedValueE (returns error) over NewCachedValue (panics if ttl <= 0) for config-driven or dynamic ttl. When ctx is cancelled the internal goroutine stops. If ctx is context.Background(), the goroutine never exits until Stop is called—call Stop when the value is no longer needed to avoid goroutine leaks.
// Get calls load with a configurable timeout (default 30s); use WithLoadTimeout(d) when constructing to override. GetStale returns only from cache. Invalidate clears the value and singleflight; an in-flight Get that finishes after Invalidate will not write back.
//
// # Key-value store
//
// KeyValueStore is a minimal Get/Set/Del interface. RedisKeyValueStore implements it with *redis.Client. Set requires ttl > 0.
// Methods return ErrRedisNotConfigured when Client is nil.
//
// # Pub/Sub
//
// PubSubStore provides Publish and Subscribe. RedisPubSubStore implements it; ChanBufferSize is the subscribe channel buffer (default 64). SendTimeout (default 30s) limits how long the subscribe goroutine waits to send a message to the returned channel; when exceeded the message is dropped and OnDrop is invoked synchronously—keep OnDrop fast to avoid blocking the subscribe loop.
// Subscribe's goroutine exits when ctx is cancelled. The caller must cancel ctx when done to avoid goroutine leaks. Correct usage:
//
//	ctx, cancel := context.WithCancel(parent)
//	defer cancel()
//	ch, err := store.Subscribe(ctx, "mychannel")
//	if err != nil { ... }
//	for msg := range ch {
//	    handle(msg)
//	}
//
// # Errors
//
// ErrRedisNotConfigured is returned by Cache, RedisKeyValueStore, and RedisPubSubStore when the Redis client is nil.
// ErrNotFound is returned by KeyValueStore.Get when the key does not exist.
package cache
