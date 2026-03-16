package cache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoundedCache_GetSet(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[string, int](10)
	_, ok := c.Get("a")
	assert.False(t, ok)
	c.Set("a", 1)
	v, ok := c.Get("a")
	require.True(t, ok)
	assert.Equal(t, 1, v)
	c.Set("b", 2)
	v, _ = c.Get("b")
	assert.Equal(t, 2, v)
	assert.Equal(t, 2, c.Len())
}

func TestBoundedCache_Eviction(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[string, int](3)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	assert.Equal(t, 3, c.Len())
	c.Set("d", 4)
	assert.Equal(t, 3, c.Len())
	_, ok := c.Get("a")
	assert.False(t, ok)
	v, ok := c.Get("d")
	require.True(t, ok)
	assert.Equal(t, 4, v)
}

func TestBoundedCache_SetOverwrites(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[string, int](5)
	c.Set("a", 1)
	c.Set("a", 2)
	assert.Equal(t, 1, c.Len())
	v, _ := c.Get("a")
	assert.Equal(t, 2, v)
}

func TestBoundedCache_SetIfAbsent_NoOverwrite(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[string, int](5)
	c.SetIfAbsent("a", 1)
	c.SetIfAbsent("a", 2)
	assert.Equal(t, 1, c.Len())
	v, _ := c.Get("a")
	assert.Equal(t, 1, v)
}

func TestBoundedCache_Concurrent(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[int, int](100)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			c.Set(n, n*2)
		}(i)
	}
	wg.Wait()
	for i := 0; i < 50; i++ {
		v, ok := c.Get(i)
		assert.True(t, ok, "key %d", i)
		assert.Equal(t, i*2, v)
	}
}

func TestBoundedCache_DefaultSize(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[string, int](0)
	require.NotNil(t, c)
	for i := 0; i < DefaultBoundedCacheSize; i++ {
		c.Set(fmt.Sprintf("k%d", i), i)
	}
	assert.Equal(t, DefaultBoundedCacheSize, c.Len())
}

func TestBoundedCache_DeleteThenSet_NoOverwrite(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[string, int](3)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	assert.Equal(t, 3, c.Len())
	c.Delete("b")
	assert.Equal(t, 2, c.Len())
	va, oka := c.Get("a")
	require.True(t, oka)
	assert.Equal(t, 1, va)
	_, okb := c.Get("b")
	assert.False(t, okb)
	vc, okc := c.Get("c")
	require.True(t, okc)
	assert.Equal(t, 3, vc)
	c.Set("d", 4)
	_, oka = c.Get("a")
	assert.False(t, oka, "a is oldest live entry so evicted when d is inserted")
	vd, okd := c.Get("d")
	require.True(t, okd)
	assert.Equal(t, 4, vd)
	vc, okc = c.Get("c")
	require.True(t, okc)
	assert.Equal(t, 3, vc)
	assert.Equal(t, 2, c.Len())
}

func TestBoundedCache_DeleteThenReinsertSameKey_NoDataLoss(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[string, int](3)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	c.Delete("b")
	c.Set("b", 99)
	c.Set("x", 100)
	v, ok := c.Get("b")
	require.True(t, ok, "b must remain after delete+reinsert+eviction of ghost")
	assert.Equal(t, 99, v)
	_, ok = c.Get("x")
	require.True(t, ok)
	assert.Equal(t, 3, c.Len())
}

func TestBoundedCache_DeleteHeadThenSet_FIFOPreserved(t *testing.T) {
	t.Parallel()
	c := NewBoundedCache[string, int](3)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	c.Delete("a")
	c.Set("d", 4)
	_, oka := c.Get("a")
	assert.False(t, oka)
	vb, okb := c.Get("b")
	require.True(t, okb)
	assert.Equal(t, 2, vb)
	vc, okc := c.Get("c")
	require.True(t, okc)
	assert.Equal(t, 3, vc)
	vd, okd := c.Get("d")
	require.True(t, okd)
	assert.Equal(t, 4, vd)
	assert.Equal(t, 3, c.Len())
	c.Set("e", 5)
	_, okb = c.Get("b")
	assert.False(t, okb, "b is oldest live so evicted first (FIFO)")
	vc, okc = c.Get("c")
	require.True(t, okc)
	assert.Equal(t, 3, vc)
	vd, okd = c.Get("d")
	require.True(t, okd)
	assert.Equal(t, 4, vd)
	ve, oke := c.Get("e")
	require.True(t, oke)
	assert.Equal(t, 5, ve)
	assert.Equal(t, 3, c.Len())
}
