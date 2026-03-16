package cache

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOrLoad_CacheHit(t *testing.T) {
	t.Parallel()
	client, mock := redismock.NewClientMock()
	c := New(client)
	ctx := context.Background()

	data := map[string]int{"x": 1}
	bytes, _ := json.Marshal(data)
	mock.ExpectGet("key").SetVal(string(bytes))

	loadCalled := false
	val, err := GetOrLoad(c, ctx, "key", time.Minute, func(context.Context) (map[string]int, error) {
		loadCalled = true
		return nil, nil
	})
	require.NoError(t, err)
	assert.Equal(t, data, val)
	assert.False(t, loadCalled)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetOrLoad_CacheMiss(t *testing.T) {
	t.Parallel()
	client, mock := redismock.NewClientMock()
	c := New(client)
	ctx := context.Background()

	mock.ExpectGet("key").SetErr(redis.Nil)
	mock.ExpectSet("key", []byte(`{"x":2}`), time.Minute).SetVal("OK")

	val, err := GetOrLoad(c, ctx, "key", time.Minute, func(context.Context) (map[string]int, error) {
		return map[string]int{"x": 2}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"x": 2}, val)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDel(t *testing.T) {
	t.Parallel()
	client, mock := redismock.NewClientMock()
	c := New(client)
	ctx := context.Background()

	mock.ExpectDel("k1", "k2").SetVal(2)
	err := c.Del(ctx, "k1", "k2")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSet(t *testing.T) {
	t.Parallel()
	client, mock := redismock.NewClientMock()
	c := New(client)
	ctx := context.Background()

	mock.ExpectSet("k", []byte(`{"N":10}`), time.Second).SetVal("OK")
	err := c.Set(ctx, "k", struct{ N int }{10}, time.Second)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteByPrefix(t *testing.T) {
	t.Parallel()
	client, mock := redismock.NewClientMock()
	c := New(client)
	ctx := context.Background()

	mock.ExpectScan(0, "p*", 500).SetVal([]string{"p1", "p2"}, 0)
	mock.ExpectUnlink("p1", "p2").SetVal(2)
	err := c.DeleteByPrefix(ctx, "p")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
