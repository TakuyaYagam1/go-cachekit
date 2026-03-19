# go-cachekit

[![CI](https://github.com/TakuyaYagam1/go-cachekit/actions/workflows/ci.yml/badge.svg)](https://github.com/TakuyaYagam1/go-cachekit/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/TakuyaYagam1/go-cachekit.svg)](https://pkg.go.dev/github.com/TakuyaYagam1/go-cachekit)
[![Go Report Card](https://goreportcard.com/badge/github.com/TakuyaYagam1/go-cachekit)](https://goreportcard.com/report/github.com/TakuyaYagam1/go-cachekit)

Redis-backed JSON cache, in-memory bounded cache, TTL single-value cache, key-value and pub/sub helpers.

## Install

```bash
go get github.com/TakuyaYagam1/go-cachekit
```

```go
import "github.com/TakuyaYagam1/go-cachekit"
```

## API

### Cache (Redis JSON + singleflight)

- **New(client)** — build Cache from go-redis Client
- **GetOrLoad[T]** — get from Redis or call loadFn, store with ttl, return; singleflight per key
- **Del** — delete keys and forget singleflight
- **Set** — marshal value as JSON, set with ttl
- **DeleteByPrefix** — scan prefix*, unlink keys, forget singleflight

### BoundedCache (in-memory LRU)

- **NewBoundedCache[K,V](maxSize)** — maxSize or DefaultBoundedCacheSize (100) if ≤ 0
- **Get**, **Set**, **Len** — no overwrite on Set if key exists

### CachedValue (single key, TTL, singleflight)

- **NewCachedValue[T](key, ttl)** — one key, ttlcache + singleflight
- **Get(ctx, load)** — cached or load(ctx), then cache
- **GetStale** — return cached value without loading
- **Invalidate** — delete and forget singleflight

### Redis client

- **RedisConfig** — Host, Port, Password, PoolSize, MinIdleConns
- **NewRedisClient(ctx, cfg)** — NewClient + Ping; error if unreachable

### Stores

- **KeyValueStore** — Get, Set, Del
- **RedisKeyValueStore** — implements KeyValueStore
- **PubSubStore** — Publish, Subscribe

## Example

```go
rdb, err := cachekit.NewRedisClient(ctx, &cachekit.RedisConfig{Host: "localhost", Port: 6379})
if err != nil {
    log.Fatal(err)
}
defer rdb.Close()

c := cachekit.New(rdb)
val, err := cachekit.GetOrLoad(c, ctx, "user:1", 5*time.Minute, func(ctx context.Context) (User, error) {
    return db.GetUser(ctx, 1)
})

mem := cachekit.NewBoundedCache[string, string](1000)
mem.Set("k", "v")
if v, ok := mem.Get("k"); ok {
    // v == "v"
}
```
