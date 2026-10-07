package main

import (
	"hash/fnv"
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
