package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShardedCache_SetGet(t *testing.T) {
	cache := NewShardedCache(4)
	cache.Set("Key", "Value", 5*time.Second)
	val, exist := cache.Get("Key")
	assert.True(t, exist)
	assert.Equal(t, "Value", val)
}
func TestSharedCache_TTL(t *testing.T) {
	cache := NewShardedCache(4)
	cache.Set("expKey", "expVal", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	val, exist := cache.Get("expKey")
	assert.False(t, exist)
	assert.Nil(t, val)
}
func TestSharedCache_GC(t *testing.T) {
	cache := NewShardedCache(4)
	defer cache.Close()

	key := "GCkey"
	cache.Set(key, "someData", 10*time.Millisecond)
	time.Sleep(75 * time.Millisecond)
	idx := cache.getShardIdx(key)

	cache.shards[idx].mu.RLock()
	_, exist := cache.shards[idx].item[key]
	cache.shards[idx].mu.RUnlock()

	assert.False(t, exist)
}
func TestShardedCache_StressConcurrency(t *testing.T) {
	cache := NewShardedCache(4)
	defer cache.Close()

	var wg sync.WaitGroup
	workersCount := 1000

	for i := 0; i < workersCount/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key_%d", id)
			cache.Set(key, id, 50*time.Millisecond)
		}(i)
	}
	for i := 0; i < workersCount/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key_%d", id)
			cache.Get(key)
		}(i)
	}
	wg.Wait()
}

//------------------------------------------------------
//benchmarks

type simpleCache struct {
	mu   sync.RWMutex
	data map[string]any
}

func NewSimpleCache() *simpleCache {
	return &simpleCache{data: make(map[string]any)}
}
func (sc *simpleCache) Set(key string, val any) {
	sc.mu.Lock()
	sc.data[key] = val
	sc.mu.Unlock()
}
func (sc *simpleCache) Get(key string) (any, bool) {
	sc.mu.RLock()
	val, exist := sc.data[key]
	sc.mu.RUnlock()
	return val, exist
}

func BenchmarkSimpleCache(b *testing.B) {
	cache := NewSimpleCache()

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			key := keys[i%1000]
			i++
			cache.Set(key, "value")
			_, _ = cache.Get(key)
		}
	})
}

func BenchmarkShardedCache(b *testing.B) {
	cache := NewShardedCache(32)
	defer cache.Close()

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			key := keys[i%1000]
			i++
			cache.Set(key, "value", 0)
			_, _ = cache.Get(key)
		}
	})
}

//-----------------------------------------------------
//server side tests and benchmarks

func TestHandleClient_GET_SET(t *testing.T) {
	cache := NewShardedCache(4)
	defer cache.Close()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	go handleClient(serverConn, cache)

	clientReader := bufio.NewReader(clientConn)

	fmt.Fprintln(clientConn, "SET key123 value123")
	setResponse, _ := clientReader.ReadString('\n')
	assert.Equal(t, "+OK\n", setResponse)

	fmt.Fprintln(clientConn, "GET key123")
	getResponse, _ := clientReader.ReadString('\n')
	assert.Equal(t, "value123\n", getResponse)
}

func BenchmarkHandleClient_Parallel(b *testing.B) {
	cache := NewShardedCache(32)
	defer cache.Close()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		serverConn, clientConn := net.Pipe()
		defer clientConn.Close()

		go handleClient(serverConn, cache)
		clientReader := bufio.NewReader(clientConn)

		for pb.Next() {
			fmt.Fprintln(clientConn, "SET benchmarkKey value 0")
			_, _ = clientReader.ReadString('\n')
		}
	})
}
