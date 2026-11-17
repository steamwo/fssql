package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func (d *Driver) OpenWrite(ctx context.Context, u vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("local: managed memory required for write")
	}
	if opts.IfNoneMatch {
		return d.OpenAtomicPublishWrite(ctx, u, opts, scope, bm)
	}
	if opts.Expected != nil {
		return nil, fmt.Errorf("%w: local compare-and-swap write is unsupported", vfs.ErrWriteUnsupported)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := d.localPath(u, false)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	return &writeHandle{file: f, finalPath: p, cleanupPath: p}, nil
}

func (d *Driver) OpenAtomicPublishWrite(ctx context.Context, u vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("local: managed memory required for atomic publish write")
	}
	if opts.Expected != nil {
		return nil, fmt.Errorf("%w: local compare-and-swap write is unsupported", vfs.ErrWriteUnsupported)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	job, err := vfs.NewPublishJobID()
	if err != nil {
		return nil, err
	}
	intent, err := d.PlanPublish(ctx, u, job, vfs.PublishStrict)
	if err != nil {
		return nil, err
	}
	h, err := d.BeginPublish(ctx, intent, func(context.Context, []byte) error { return nil }, scope, bm)
	if err != nil {
		if h != nil {
			_ = h.Release()
		}
		return nil, err
	}
	h.(*localPublishHandle).legacyReplace = !opts.IfNoneMatch
	h.(*localPublishHandle).legacyWrite = true
	return vfs.NewPublishWriteAdapter(job, h, func(ctx context.Context) (vfs.ObjectIdentity, error) {
		info, err := d.Stat(ctx, u)
		return info.Identity, err
	}), nil
}

func (d *Driver) Delete(ctx context.Context, u vfs.URI, expected *vfs.ObjectIdentity) error {
	if expected != nil {
		return fmt.Errorf("%w: local conditional delete is not advertised", vfs.ErrWriteUnsupported)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := d.localPath(u, true)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

type writeHandle struct {
	mu          sync.Mutex
	file        *os.File
	finalPath   string
	cleanupPath string
	atomic      bool
	noReplace   bool
	failed      error
	terminal    bool
}

func (h *writeHandle) Write(ctx context.Context, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal || h.file == nil {
		return 0, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		return 0, h.failed
	}
	if err := ctx.Err(); err != nil {
		h.failed = err
		return 0, err
	}
	n, err := h.file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		h.failed = err
	}
	return n, err
}

func (h *writeHandle) Close(ctx context.Context) (vfs.ObjectIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal || h.file == nil {
		return vfs.ObjectIdentity{}, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		return vfs.ObjectIdentity{}, h.failed
	}
	if err := ctx.Err(); err != nil {
		h.failed = err
		return vfs.ObjectIdentity{}, err
	}
	if err := h.file.Sync(); err != nil {
		h.failed = err
		return vfs.ObjectIdentity{}, err
	}
	info, err := h.file.Stat()
	if err != nil {
		h.failed = err
		return vfs.ObjectIdentity{}, err
	}
	identity := infoIdentity(info)
	if err := h.file.Close(); err != nil {
		h.file = nil
		h.failed = err
		return vfs.ObjectIdentity{}, err
	}
	h.file = nil
	if h.atomic {
		publish := publishFile
		if h.noReplace {
			publish = publishFileIfAbsent
		}
		if err := publish(h.cleanupPath, h.finalPath); err != nil {
			h.failed = err
			return vfs.ObjectIdentity{}, err
		}
		h.cleanupPath = ""
	}
	h.terminal = true
	return identity, nil
}

// Linking a completed staging inode creates the final name atomically and
// fails if another writer wins. A prior Stat would leave an overwrite race.
func publishFileIfAbsent(stagingPath, finalPath string) error {
	if err := os.Link(stagingPath, finalPath); err != nil {
		return err
	}
	// Publication already succeeded; a cleanup error cannot undo that outcome.
	_ = os.Remove(stagingPath)
	if dir, err := os.Open(filepath.Dir(finalPath)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func (h *writeHandle) Abort(ctx context.Context) error {
	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return nil
	}
	var closeErr error
	if h.file != nil {
		closeErr = h.file.Close()
		h.file = nil
	}
	cleanup := h.cleanupPath
	h.cleanupPath = ""
	h.terminal = true
	h.mu.Unlock()
	if cleanup == "" {
		return closeErr
	}
	removeErr := os.Remove(cleanup)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(closeErr, removeErr, err)
	}
	return errors.Join(closeErr, removeErr)
}
