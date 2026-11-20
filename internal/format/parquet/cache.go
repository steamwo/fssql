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
	URI        string
	ViewKey    string
	SnapshotID string
	VersionID  string
	ETag       string
	Size       int64
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
	shared     *MetadataCache
	queryLocal bool
	readKeys   ReadKeyResolver
	mu         sync.Mutex
	maxEntries int
	lru        *list.List
	entries    map[metadataCacheKey]*list.Element
	flights    map[metadataCacheKey]*metadataFlight
	hits       uint64
	misses     uint64
	evictions  uint64
}

// NewQueryMetadataCache keeps encrypted, HTTP, and materialized footers within
// the query while sharing plaintext footers for other physical objects.
func NewQueryMetadataCache(shared *MetadataCache, readKeys ReadKeyResolver) *MetadataCache {
	local, _ := NewMetadataCache(shared.maxEntries)
	local.shared, local.queryLocal, local.readKeys = shared, true, readKeys
	return local
}

func (c *MetadataCache) route(src FooterSource) *MetadataCache {
	if c == nil {
		return nil
	}
	if c.shared != nil && !src.URI.IsHTTP() && src.Identity.SnapshotID == "" {
		return c.shared
	}
	return c
}

func (c *MetadataCache) key(src FooterSource) (metadataCacheKey, bool) {
	if (src.URI.IsHTTP() || src.Identity.SnapshotID != "") && !c.queryLocal {
		return metadataCacheKey{}, false
	}
	return metadataKey(src)
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
	if !src.Identity.Stable() && src.Identity.SnapshotID == "" {
		return metadataCacheKey{}, false
	}
	return metadataCacheKey{
		URI:        src.URI.IdentityKey(),
		ViewKey:    src.URI.ViewKey,
		SnapshotID: src.Identity.SnapshotID,
		VersionID:  src.Identity.VersionID,
		ETag:       src.Identity.ETag,
		Size:       src.Size,
	}, true
}

func (c *MetadataCache) Get(src FooterSource) (Footer, bool) {
	return c.route(src).lookup(src)
}

func (c *MetadataCache) lookup(src FooterSource) (Footer, bool) {
	if c == nil {
		return Footer{}, false
	}
	key, cacheable := c.key(src)
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
	c = c.route(src)
	if c == nil {
		return false
	}
	key, cacheable := c.key(src)
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
	if shared := c.route(src); shared != c {
		if c.readKeys != nil {
			if footer, ok := c.lookup(src); ok {
				return footer, nil
			}
		}
		footer, err := shared.getOrDiscover(ctx, driver, src, scope, bm, maxFooterBytes)
		if c.readKeys == nil || !errors.Is(err, errEncryptedKeyRequired) {
			return footer, err
		}
		// The shared cache only discovers plaintext footers. Authenticated
		// metadata and the decryption state belong to this query.
	}
	return c.getOrDiscover(ctx, driver, src, scope, bm, maxFooterBytes)
}

func (c *MetadataCache) getOrDiscover(ctx context.Context, driver vfs.Driver, src FooterSource, scope *memory.Scope, bm *memory.BufferManager, maxFooterBytes int64) (Footer, error) {
	key, cacheable := c.key(src)
	if !cacheable {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		return discoverFooter(ctx, driver, src, scope, bm, maxFooterBytes, c.readKeys)
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

	footer, err := discoverFooter(ctx, driver, src, scope, bm, maxFooterBytes, c.readKeys)

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
