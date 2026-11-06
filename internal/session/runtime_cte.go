package session

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/blocking"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type cteCache struct {
	mu    sync.Mutex
	ready bool
	store *storagetemp.SpillablePageStore
	pages []storagetemp.PageID
}

var cteCaches = struct {
	sync.Mutex
	byFactory map[*ExternalSortFactory]map[string]*cteCache
}{byFactory: make(map[*ExternalSortFactory]map[string]*cteCache)}

func cteCacheFor(f *ExternalSortFactory, key string) (*cteCache, error) {
	cteCaches.Lock()
	defer cteCaches.Unlock()
	m := cteCaches.byFactory[f]
	if m == nil {
		m = make(map[string]*cteCache)
		cteCaches.byFactory[f] = m
		if err := f.query.AddCleanup(func() error { return closeCTECaches(f) }); err != nil {
			delete(cteCaches.byFactory, f)
			return nil, err
		}
	}
	cache := m[key]
	if cache == nil {
		cache = &cteCache{}
		m[key] = cache
	}
	return cache, nil
}

func closeCTECaches(f *ExternalSortFactory) error {
	cteCaches.Lock()
	m := cteCaches.byFactory[f]
	delete(cteCaches.byFactory, f)
	cteCaches.Unlock()
	var errs []error
	for _, cache := range m {
		cache.mu.Lock()
		if cache.store != nil { errs = append(errs, cache.store.Close()); cache.store = nil }
		cache.pages = nil
		cache.mu.Unlock()
	}
	return errors.Join(errs...)
}

type cteOperator struct {
	factory   *ExternalSortFactory
	cache     *cteCache
	store     *storagetemp.SpillablePageStore
	pages     []storagetemp.PageID
	published bool
}

var _ blocking.Operator = (*cteOperator)(nil)

func (o *cteOperator) ReuseReady() bool {
	if o == nil || o.cache == nil {
		return false
	}
	o.cache.mu.Lock()
	defer o.cache.mu.Unlock()
	return o.cache.ready
}

func newCTEOperator(f *ExternalSortFactory, key string) (blocking.Operator, error) {
	cache, err := cteCacheFor(f, key)
	if err != nil { return nil, err }
	store, err := storagetemp.NewSpillablePageStore(f.query.Temp, f.query.Memory, f.buffers, storagetemp.PageStoreConfig{SegmentMaxBytes:64 << 20, Compression:storagetemp.CompressionNone})
	if err != nil { return nil, err }
	return &cteOperator{factory:f, cache:cache, store:store}, nil
}

func (o *cteOperator) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if o == nil || o.store == nil || chunk == nil { return fmt.Errorf("session: invalid CTE materializer") }
	if err := ctx.Err(); err != nil { return err }
	encoded, err := vector.SerializeChunk(chunk, o.factory.query.Memory, o.factory.buffers)
	if err != nil { return err }
	defer encoded.Release()
	page, err := o.store.WritePage(ctx, encoded.Bytes())
	if err != nil { return err }
	o.pages = append(o.pages, page)
	return nil
}

func (o *cteOperator) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	if o == nil || o.cache == nil { return fmt.Errorf("session: invalid CTE cache") }
	o.cache.mu.Lock()
	if !o.cache.ready {
		o.cache.ready = true
		o.cache.store = o.store
		o.cache.pages = append([]storagetemp.PageID(nil), o.pages...)
		o.published = true
	}
	store := o.cache.store
	pages := append([]storagetemp.PageID(nil), o.cache.pages...)
	o.cache.mu.Unlock()
	if store == nil { return fmt.Errorf("session: CTE cache has no storage") }
	for _, id := range pages {
		if err := ctx.Err(); err != nil { return err }
		page, err := store.ReadPage(ctx, id)
		if err != nil { return err }
		chunk, err := vector.DeserializeChunk(page.Bytes(), o.factory.query.Memory, o.factory.buffers)
		page.Release()
		if err != nil { return err }
		if err := emit(chunk); err != nil { chunk.Release(); return err }
	}
	return nil
}

func (o *cteOperator) Release() {
	if o == nil { return }
	if o.store != nil && !o.published { _ = o.store.Close() }
	o.store = nil
	o.pages = nil
}
