package cache

import (
	"sync"
	"time"
)

type Cache struct {
	items map[string]cacheItem
	mx    sync.RWMutex
}

type cacheItem struct {
	val      interface{}
	err      error
	expireAt time.Time
}

func (cache *Cache) Clear() {
	cache.mx.Lock()
	cache.items = make(map[string]cacheItem)
	cache.mx.Unlock()
}

func (cache *Cache) Remeber(key string, ttl time.Duration, cb func() (interface{}, error)) (interface{}, error) {

	cache.mx.RLock()
	if curItem, exists := cache.items[key]; exists && curItem.expireAt.After(time.Now()) {
		cache.mx.RUnlock()

		if curItem.err != nil {
			return nil, curItem.err
		}

		return curItem.val, nil
	}
	cache.mx.RUnlock()

	val, err := cb()

	cache.mx.Lock()
	cache.items[key] = cacheItem{
		val:      val,
		err:      err,
		expireAt: time.Now().Add(ttl),
	}
	cache.mx.Unlock()

	if err != nil {
		return nil, err
	}

	return val, nil
}

func GetCache() Cache {
	return Cache{
		items: make(map[string]cacheItem),
	}
}
