package cache

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRedisClient_Error(t *testing.T) {
	t.Parallel()
	_, err := NewRedisClient(context.Background(), &RedisConfig{Host: "127.0.0.1", Port: "1"})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "redis connection failed"))
}
