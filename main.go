package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"net"
	"strings"
	"sync"
	"time"
)

type ShardedCache struct {
	shardsCount uint32
	shards      []*cacheShard
	stopChan    chan struct{}
}

func (sc *ShardedCache) startCleanup(idx uint32, interval time.Duration, stopch chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			shard := sc.shards[idx]

			shard.mu.Lock()
			for key, item := range shard.item {
				if item.expiresAt > 0 && time.Now().UnixNano() > item.expiresAt {
					delete(shard.item, key)
				}
			}
			shard.mu.Unlock()
		case <-stopch:
			return
		}
	}
}
func (sc *ShardedCache) Close() {
	close(sc.stopChan)
}
func (sc *ShardedCache) getShardIdx(key string) uint32 {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return hasher.Sum32() % sc.shardsCount

}
func (sc *ShardedCache) Set(key string, value any, ttl time.Duration) {
	idx := sc.getShardIdx(key)
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}
	sc.shards[idx].mu.Lock()
	defer sc.shards[idx].mu.Unlock()

	sc.shards[idx].item[key] = cacheItem{
		value:     value,
		expiresAt: expiresAt,
	}
}
func (sc *ShardedCache) Get(key string) (any, bool) {
	idx := sc.getShardIdx(key)

	sc.shards[idx].mu.RLock()
	defer sc.shards[idx].mu.RUnlock()

	item, exist := sc.shards[idx].item[key]
	if !exist {
		return nil, false
	}

	if item.expiresAt > 0 && time.Now().UnixNano() > item.expiresAt {
		return nil, false
	}

	return item.value, true
}
func (sc *ShardedCache) Delete(key string) {
	idx := sc.getShardIdx(key)

	sc.shards[idx].mu.Lock()
	defer sc.shards[idx].mu.Unlock()

	delete(sc.shards[idx].item, key)
}

// --------------------------------------

type cacheShard struct {
	mu   sync.RWMutex
	item map[string]cacheItem
}

type cacheItem struct {
	value     any
	expiresAt int64
}

func NewShardedCache(sc uint32) *ShardedCache {
	shards := make([]*cacheShard, sc)
	for i := uint32(0); i < sc; i++ {
		shards[i] = &cacheShard{
			item: make(map[string]cacheItem),
		}
	}
	stopChan := make(chan struct{})
	ShardedCache := ShardedCache{
		shardsCount: sc,
		shards:      shards,
		stopChan:    stopChan,
	}
	for i := uint32(0); i < sc; i++ {
		go ShardedCache.startCleanup(i, 50*time.Millisecond, stopChan)
	}
	return &ShardedCache
}
func main() {
	cache := NewShardedCache(32)
	defer cache.Close()

	listener, err := net.Listen("tcp", ":6379")

	if err != nil {
		fmt.Printf("не удалось запустить сервер %v\n", err)
		return
	}

	defer listener.Close()
	fmt.Println("Сервер успешно запущен на порту: 6379")

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Printf("Ошибка при принятии подключения: %v\n", err)
			continue
		}

		go handleClient(conn, cache)
	}
}

func handleClient(conn net.Conn, cache *ShardedCache) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		input, err := reader.ReadString('\n')

		if err != nil {
			fmt.Printf("клиент %s отключился \n", conn.RemoteAddr().String())
			return
		}
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		parts := strings.Fields(input)
		command := strings.ToUpper(parts[0])
		if len(parts) < 3 {
			fmt.Fprintln(conn, "переданы некорректные аргументы")
			continue
		}
		switch command {
		case "GET":
			val, exist := cache.Get(parts[1])
			if !exist {
				fmt.Fprintln(conn, "(nil)")
			} else {
				fmt.Fprintln(conn, val)
			}
		case "SET":
			var ttl time.Duration

			if len(parts) > 3 {
				var seconds int
				_, err := fmt.Sscanf(parts[3], "%d", &seconds)
				if err == nil && seconds > 0 {
					ttl = time.Duration(seconds) * time.Second
				}
			}
			cache.Set(parts[1], parts[2], ttl)
			fmt.Fprint(conn, "+OK")

		case "DELETE":
			cache.Delete(parts[1])
			fmt.Fprintln(conn, "+OK")
		}
	}
}
