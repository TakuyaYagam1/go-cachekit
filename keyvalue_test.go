package cache

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisKeyValueStore_NilClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := &RedisKeyValueStore{}
	val, err := s.Get(ctx, "k")
	assert.ErrorIs(t, err, ErrRedisNotConfigured)
	assert.Empty(t, val)

	err = s.Set(ctx, "k", []byte("v"), time.Minute)
	assert.ErrorIs(t, err, ErrRedisNotConfigured)

	err = s.Del(ctx, "k")
	assert.ErrorIs(t, err, ErrRedisNotConfigured)
}

func TestRedisKeyValueStore_NilReceiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var s *RedisKeyValueStore
	_, err := s.Get(ctx, "k")
	assert.ErrorIs(t, err, ErrRedisNotConfigured)
	err = s.Set(ctx, "k", []byte("v"), time.Minute)
	assert.ErrorIs(t, err, ErrRedisNotConfigured)
	err = s.Del(ctx, "k")
	assert.ErrorIs(t, err, ErrRedisNotConfigured)
}

func TestRedisKeyValueStore_Get_NotFound_ReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	client, mock := redismock.NewClientMock()
	mock.ExpectGet("missing").SetErr(redis.Nil)
	store := &RedisKeyValueStore{Client: client}
	val, err := store.Get(context.Background(), "missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Empty(t, val)
	require.NoError(t, mock.ExpectationsWereMet())
}
