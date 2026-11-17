package vfs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"
	"time"
)

// NewPublishJobID is also used by legacy adapters that have no ManifestStore.
func NewPublishJobID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

// NewPublishWriteAdapter preserves the legacy Close API over a staged handle.
// It has no durable store; public COPY must use its locked coordinator instead.
func NewPublishWriteAdapter(job string, h PublishHandle, identity func(context.Context) (ObjectIdentity, error)) WriteHandle {
	return &publishWriteAdapter{job: job, handle: h, identity: identity, digest: sha256.New()}
}

type publishWriteAdapter struct {
	mu                  sync.Mutex
	job                 string
	handle              PublishHandle
	identity            func(context.Context) (ObjectIdentity, error)
	digest              hash.Hash
	written             int64
	sealed              *SealedObject
	failed              error
	terminal, uncertain bool
}

func (h *publishWriteAdapter) Write(ctx context.Context, p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal {
		return 0, ErrWriteClosed
	}
	if h.failed != nil {
		return 0, h.failed
	}
	n, err := h.handle.Write(ctx, p)
	if n < 0 || n > len(p) {
		n = 0
		err = io.ErrShortWrite
	}
	if n > 0 {
		_, _ = h.digest.Write(p[:n])
		h.written += int64(n)
	}
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	h.failed = err
	return n, err
}

func (h *publishWriteAdapter) Close(ctx context.Context) (ObjectIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal {
		return ObjectIdentity{}, ErrWriteClosed
	}
	if h.failed != nil {
		return ObjectIdentity{}, h.failed
	}
	defer h.handle.Release()
	s, err := h.handle.Seal(ctx, WriteEvidence{Size: h.written, Checksum: hex.EncodeToString(h.digest.Sum(nil))})
	if err != nil {
		h.failed = err
		return ObjectIdentity{}, err
	}
	h.sealed = &s
	_, err = h.handle.Publish(ctx, s)
	if err != nil {
		if errors.Is(err, ErrPublishConflict) || errors.Is(err, ErrPublishFailed) || errors.Is(err, ErrInvalidSealedObject) {
			h.failed = err
			return ObjectIdentity{}, err
		}
		h.uncertain = true
		h.failed = &CommitUncertainError{JobID: h.job, Err: err}
		return ObjectIdentity{}, h.failed
	}
	id, err := h.identity(ctx)
	if err == nil && id.Size != h.written {
		err = io.ErrShortWrite
	}
	if err != nil {
		h.uncertain = true
		h.failed = &CommitUncertainError{JobID: h.job, Err: err}
		return ObjectIdentity{}, h.failed
	}
	h.terminal = true
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = h.handle.Abort(cleanup, h.sealed)
	return id, nil
}

func (h *publishWriteAdapter) Abort(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal {
		return nil
	}
	h.terminal = true
	defer h.handle.Release()
	if h.uncertain {
		return nil
	}
	return h.handle.Abort(ctx, h.sealed)
}

// Release discards only in-process state, including on process-loss simulation.
func (h *publishWriteAdapter) Release() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.terminal = true
	return h.handle.Release()
}
