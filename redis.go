package cache

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig holds connection and pool settings for NewRedisClient.
// Use String or GoString when logging to avoid exposing Password.
type RedisConfig struct {
	Host         string      // Redis host (required).
	Port         string      // Redis port (required).
	Password     string      // Optional password; use GoString() when logging.
	DB           int         // Redis database number; 0 is default.
	PoolSize     int         // Max connections in pool; zero uses default (50).
	MinIdleConns int         // Min idle connections; zero uses default (10).
	TLSConfig    *tls.Config // Optional TLS; when set, use TLS to connect.
}

// String implements fmt.Stringer so that Password is not leaked in logs (%v, %+v, %s).
func (c RedisConfig) String() string {
	return c.GoString()
}

// GoString implements fmt.GoStringer so that Password is not leaked in logs (%#v).
func (c RedisConfig) GoString() string {
	tlsStr := "nil"
	if c.TLSConfig != nil {
		tlsStr = "non-nil"
	}
	return fmt.Sprintf("RedisConfig{Host:%q, Port:%q, DB:%d, Password:\"***\", PoolSize:%d, MinIdleConns:%d, TLSConfig:%s}",
		c.Host, c.Port, c.DB, c.PoolSize, c.MinIdleConns, tlsStr)
}

const (
	redisDialTimeout  = 5 * time.Second
	redisReadTimeout  = 3 * time.Second
	redisWriteTimeout = 3 * time.Second
	redisPingTimeout  = 5 * time.Second
	redisDefaultPool  = 50
	redisDefaultIdle  = 10
)

// NewRedisClient creates a Redis client and pings the server; returns an error if unreachable.
// Ping uses the shorter of ctx's deadline and redisPingTimeout; ctx cancellation aborts the ping.
func NewRedisClient(ctx context.Context, cfg *RedisConfig) (*redis.Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("redis config is nil")
	}
	if cfg.Host == "" || cfg.Port == "" {
		return nil, fmt.Errorf("redis host and port are required")
	}
	portNum, err := strconv.Atoi(cfg.Port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return nil, fmt.Errorf("redis port must be 1-65535, got %q", cfg.Port)
	}
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = redisDefaultPool
	}
	minIdle := cfg.MinIdleConns
	if minIdle <= 0 {
		minIdle = redisDefaultIdle
	}
	opts := &redis.Options{
		Addr:         cfg.Host + ":" + cfg.Port,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  redisDialTimeout,
		ReadTimeout:  redisReadTimeout,
		WriteTimeout: redisWriteTimeout,
		PoolSize:     poolSize,
		MinIdleConns: minIdle,
	}
	if cfg.TLSConfig != nil {
		opts.TLSConfig = cfg.TLSConfig
	}
	rdb := redis.NewClient(opts)

	pingCtx, cancel := context.WithTimeout(ctx, redisPingTimeout)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		if closeErr := rdb.Close(); closeErr != nil {
			return nil, fmt.Errorf("redis connection failed: %w (close: %v)", err, closeErr)
		}
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return rdb, nil
}
