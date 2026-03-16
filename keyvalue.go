package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by KeyValueStore.Get when the key does not exist.
var ErrNotFound = errors.New("cache: key not found")

// KeyValueStore is a minimal get/set/del store.
// Get returns string (Redis and most backends expose values as strings); Set accepts []byte so callers can pass serialized payloads (e.g. json.Marshal) without conversion.
type KeyValueStore interface {
	// Get returns the value for key as a string, or an error if missing or the store is unavailable.
	Get(ctx context.Context, key string) (string, error)
	// Set stores value at key with the given ttl. ttl must be positive. Value is raw bytes (e.g. JSON); no encoding is applied.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Del removes the given keys. No-op if keys is empty.
	Del(ctx context.Context, keys ...string) error
}

// RedisKeyValueStore implements KeyValueStore using a Redis client.
// Client must be non-nil and must not be replaced after creation; otherwise methods return ErrRedisNotConfigured.
type RedisKeyValueStore struct {
	Client *redis.Client
}

var _ KeyValueStore = (*RedisKeyValueStore)(nil)

// Get returns the value for key from Redis. Returns ErrNotFound when the key does not exist, ErrRedisNotConfigured if Client is nil. Key must be non-empty.
func (r *RedisKeyValueStore) Get(ctx context.Context, key string) (string, error) {
	if r == nil || r.Client == nil {
		return "", ErrRedisNotConfigured
	}
	if key == "" {
		return "", fmt.Errorf("keyvalue get: key must be non-empty")
	}
	val, err := r.Client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("keyvalue get: %w", err)
	}
	return val, nil
}

// Set writes value at key with ttl. ttl must be positive. Returns ErrRedisNotConfigured if Client is nil. Key must be non-empty.
func (r *RedisKeyValueStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if r == nil || r.Client == nil {
		return ErrRedisNotConfigured
	}
	if key == "" {
		return fmt.Errorf("keyvalue set: key must be non-empty")
	}
	if ttl <= 0 {
		return fmt.Errorf("keyvalue set: ttl must be positive, got %v", ttl)
	}
	if err := r.Client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("keyvalue set: %w", err)
	}
	return nil
}

// Del removes the given keys from Redis. Returns ErrRedisNotConfigured if Client is nil. All keys must be non-empty.
func (r *RedisKeyValueStore) Del(ctx context.Context, keys ...string) error {
	if r == nil || r.Client == nil {
		return ErrRedisNotConfigured
	}
	if len(keys) == 0 {
		return nil
	}
	for _, k := range keys {
		if k == "" {
			return fmt.Errorf("keyvalue del: key must be non-empty")
		}
	}
	if err := r.Client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("keyvalue del: %w", err)
	}
	return nil
}
