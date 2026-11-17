package objectview

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/compression"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var ErrCompressionMismatch = errors.New("objectview: file compression magic does not match")

// pipeline owns a single physical stream and all intermediate decoder state.
// Cancel closes only physical I/O; decoder workspaces are freed after Read exits.
type pipeline struct {
	io.Reader
	root     *onceReadCloser
	stop     func() bool
	cleanups []func() error
	decoded  bool
}

func (p *pipeline) Cancel() { _ = p.root.Close() }
func (p *pipeline) Close() error {
	p.stop()
	var errs []error
	errs = append(errs, p.root.Close())
	for i := len(p.cleanups) - 1; i >= 0; i-- {
		errs = append(errs, p.cleanups[i]())
	}
	p.cleanups = nil
	return errors.Join(errs...)
}

func (r *Resolver) prepareInput(ctx context.Context, input io.ReadCloser, size int64, encoding, fileMode string) (_ *pipeline, resultErr error) {
	p := &pipeline{root: &onceReadCloser{ReadCloser: input}}
	p.stop = context.AfterFunc(ctx, p.Cancel)
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, p.Close())
		}
	}()
	p.Reader = &physicalStream{ctx: ctx, reader: p.root, size: size}
	addDecoder := func(kind string) error {
		d, err := compression.NewManagedReader(ctx, kind, p.Reader, r.q.Memory)
		if err != nil {
			return err
		}
		p.Reader = d
		p.cleanups = append(p.cleanups, d.Close)
		p.decoded = true
		return nil
	}
	if encoding == "gzip" {
		if err := addDecoder("gzip"); err != nil {
			return nil, err
		}
	} else if encoding != "" && encoding != "identity" {
		return nil, errors.New("objectview: unsupported transport encoding")
	}
	if fileMode == "none" {
		return p, nil
	}
	buf, err := r.buffers.Allocate(r.q.Memory, memory.TagCompression, 4)
	if err != nil {
		return nil, err
	}
	p.cleanups = append(p.cleanups, func() error { buf.Release(); return nil })
	n := 0
	for n < buf.Len() {
		count, readErr := p.Reader.Read(buf.Bytes()[n:])
		n += count
		if readErr != nil {
			if readErr != io.EOF {
				return nil, readErr
			}
			break
		}
		if count == 0 {
			return nil, io.ErrNoProgress
		}
	}
	kind := sniffCompression(buf.Bytes()[:n])
	p.Reader = io.MultiReader(bytes.NewReader(buf.Bytes()[:n]), p.Reader)
	if fileMode == "auto" {
		fileMode = kind
	} else if fileMode != kind {
		return nil, ErrCompressionMismatch
	}
	if fileMode != "none" {
		if err := addDecoder(fileMode); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func sniffCompression(prefix []byte) string {
	if len(prefix) >= 2 && prefix[0] == 0x1f && prefix[1] == 0x8b {
		return "gzip"
	}
	if len(prefix) >= 4 {
		magic := binary.LittleEndian.Uint32(prefix[:4])
		if magic == 0xfd2fb528 || magic >= 0x184d2a50 && magic <= 0x184d2a5f {
			return "zstd"
		}
	}
	return "none"
}

type physicalStream struct {
	ctx        context.Context
	reader     io.Reader
	size, read int64
}

func (s *physicalStream) Read(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := s.reader.Read(p)
	// Cancellation may close the source before its blocked Read wakes up. Keep
	// the caller's cancellation cause instead of the resulting transport error.
	if err != nil {
		if canceled := s.ctx.Err(); canceled != nil {
			return n, canceled
		}
	}
	s.read += int64(n)
	if s.size >= 0 && (s.read > s.size || err == io.EOF && s.read != s.size) {
		return n, vfs.ErrShortRead
	}
	return n, err
}
