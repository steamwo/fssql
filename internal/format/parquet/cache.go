package parquet

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type MetadataCacheMetrics struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Entries   int
}

type metadataCacheKey struct {
	URI       string
	VersionID string
	ETag      string
	Size      int64
}

type metadataCacheEntry struct {
	key    metadataCacheKey
	footer Footer
}

type metadataFlight struct {
	done   chan struct{}
	footer Footer
	err    error
}

// MetadataCache is a bounded, concurrency-safe LRU of immutable Parquet footer
// results. Only stable object identities are cacheable; callers with an unstable
// identity always rediscover the footer to avoid reusing stale schema metadata.
type MetadataCache struct {
	mu         sync.Mutex
	maxEntries int
	lru        *list.List
	entries    map[metadataCacheKey]*list.Element
	flights    map[metadataCacheKey]*metadataFlight
	hits       uint64
	misses     uint64
	evictions  uint64
}

func NewMetadataCache(maxEntries int) (*MetadataCache, error) {
	if maxEntries <= 0 {
		return nil, errors.New("parquet: metadata cache capacity must be positive")
	}
	return &MetadataCache{
		maxEntries: maxEntries,
		lru:        list.New(),
		entries:    make(map[metadataCacheKey]*list.Element, maxEntries),
		flights:    make(map[metadataCacheKey]*metadataFlight),
	}, nil
}

func metadataKey(src FooterSource) (metadataCacheKey, bool) {
	if !src.Identity.Stable() {
		return metadataCacheKey{}, false
	}
	return metadataCacheKey{
		URI:       src.URI.String(),
		VersionID: src.Identity.VersionID,
		ETag:      src.Identity.ETag,
		Size:      src.Size,
	}, true
}

func (c *MetadataCache) Get(src FooterSource) (Footer, bool) {
	if c == nil {
		return Footer{}, false
	}
	key, cacheable := metadataKey(src)
	if !cacheable {
		return Footer{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return Footer{}, false
	}
	c.lru.MoveToFront(el)
	c.hits++
	return el.Value.(metadataCacheEntry).footer, true
}

func (c *MetadataCache) Put(src FooterSource, footer Footer) bool {
	if c == nil {
		return false
	}
	key, cacheable := metadataKey(src)
	if !cacheable {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putLocked(key, footer)
	return true
}

func (c *MetadataCache) putLocked(key metadataCacheKey, footer Footer) {
	if el, ok := c.entries[key]; ok {
		el.Value = metadataCacheEntry{key: key, footer: footer}
		c.lru.MoveToFront(el)
		return
	}
	el := c.lru.PushFront(metadataCacheEntry{key: key, footer: footer})
	c.entries[key] = el
	if c.lru.Len() <= c.maxEntries {
		return
	}
	oldest := c.lru.Back()
	if oldest == nil {
		return
	}
	entry := oldest.Value.(metadataCacheEntry)
	delete(c.entries, entry.key)
	c.lru.Remove(oldest)
	c.evictions++
}

// GetOrDiscover reuses DiscoverFooter as the sole metadata/schema inference
// path. Concurrent misses for the same stable object are coalesced into one
// footer discovery. Waiting callers may cancel without cancelling the owner.
func (c *MetadataCache) GetOrDiscover(ctx context.Context, driver vfs.Driver, src FooterSource, scope *memory.Scope, bm *memory.BufferManager, maxFooterBytes int64) (Footer, error) {
	if c == nil {
		return DiscoverFooter(ctx, driver, src, scope, bm, maxFooterBytes)
	}
	key, cacheable := metadataKey(src)
	if !cacheable {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		return DiscoverFooter(ctx, driver, src, scope, bm, maxFooterBytes)
	}

	c.mu.Lock()
	if el, ok := c.entries[key]; ok {
		c.lru.MoveToFront(el)
		c.hits++
		footer := el.Value.(metadataCacheEntry).footer
		c.mu.Unlock()
		return footer, nil
	}
	if flight, ok := c.flights[key]; ok {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return Footer{}, ctx.Err()
		case <-flight.done:
			return flight.footer, flight.err
		}
	}
	flight := &metadataFlight{done: make(chan struct{})}
	c.flights[key] = flight
	c.misses++
	c.mu.Unlock()

	footer, err := DiscoverFooter(ctx, driver, src, scope, bm, maxFooterBytes)

	c.mu.Lock()
	if err == nil {
		c.putLocked(key, footer)
	}
	flight.footer = footer
	flight.err = err
	delete(c.flights, key)
	close(flight.done)
	c.mu.Unlock()
	return footer, err
}

func (c *MetadataCache) Metrics() MetadataCacheMetrics {
	if c == nil {
		return MetadataCacheMetrics{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return MetadataCacheMetrics{Hits: c.hits, Misses: c.misses, Evictions: c.evictions, Entries: len(c.entries)}
}

func (c *MetadataCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[metadataCacheKey]*list.Element, c.maxEntries)
	c.lru.Init()
}
