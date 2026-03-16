package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrRedisNotConfigured is returned by Cache, RedisKeyValueStore, and RedisPubSubStore when the Redis client is nil.
var ErrRedisNotConfigured = errors.New("redis client not configured")

// OnDropFunc is called synchronously when a message is dropped because SendTimeout was exceeded; return quickly to avoid blocking the subscribe loop. Optional; nil means no callback.
type OnDropFunc func(channel, payload string)

// RedisPubSubStore implements PubSubStore using a Redis client.
// Client must be non-nil and must not be replaced after creation. ChanBufferSize is the buffer size for the channel returned by Subscribe (default 64 if <= 0).
// SendTimeout, when > 0, limits how long the goroutine waits to send a message to the returned channel; if exceeded, the message is dropped and OnDrop is called if set.
type RedisPubSubStore struct {
	Client         *redis.Client
	ChanBufferSize int
	SendTimeout    time.Duration
	OnDrop         OnDropFunc
}

var _ PubSubStore = (*RedisPubSubStore)(nil)

// Publish sends message to channel. Returns ErrRedisNotConfigured if Client is nil.
func (r *RedisPubSubStore) Publish(ctx context.Context, channel, message string) error {
	if r == nil || r.Client == nil {
		return ErrRedisNotConfigured
	}
	return r.Client.Publish(ctx, channel, message).Err()
}

// Subscribe returns a channel of messages for the given channel. The goroutine receiving from Redis
// exits when ctx is cancelled or when the subscription is closed. The caller MUST cancel ctx when
// done (e.g. defer cancel()) to release the goroutine and close the returned channel; otherwise the
// goroutine will block when the channel buffer is full and will leak. Do not stop reading from the
// returned channel without cancelling ctx. See package doc for a usage example.
func (r *RedisPubSubStore) Subscribe(ctx context.Context, channel string) (<-chan string, error) {
	if r == nil || r.Client == nil {
		return nil, ErrRedisNotConfigured
	}
	pubsub := r.Client.Subscribe(ctx, channel)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("pubsub subscribe: %w", err)
	}
	buf := r.ChanBufferSize
	if buf <= 0 {
		buf = 64
	}
	sendTimeout := r.SendTimeout
	if sendTimeout <= 0 {
		sendTimeout = 30 * time.Second
	}
	out := make(chan string, buf)
	onDrop := r.OnDrop
	go func() {
		defer close(out)
		defer pubsub.Close()
		var timer *time.Timer
		for {
			select {
			case <-ctx.Done():
				if timer != nil {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
				}
				return
			case msg, ok := <-pubsub.Channel():
				if !ok {
					if timer != nil {
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
					}
					return
				}
				if timer == nil {
					timer = time.NewTimer(sendTimeout)
				} else {
					timer.Reset(sendTimeout)
				}
				select {
				case out <- msg.Payload:
					if !timer.Stop() {
						<-timer.C
					}
				case <-ctx.Done():
					if timer != nil {
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
					}
					return
				case <-timer.C:
					if onDrop != nil {
						onDrop(channel, msg.Payload)
					}
				}
			}
		}
	}()
	return out, nil
}
