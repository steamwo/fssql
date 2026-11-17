package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// PublishSession is optional; a read/write-only session cannot silently emulate
// exclusive creation or the OpenSSH no-replace hardlink extension.
type PublishSession interface {
	WritableSession
	PublishExtensions() (uint32, string)
	CreateExclusive(context.Context, string) error
	WriteExisting(context.Context, string, []byte, int64) (int, error)
	Rename(context.Context, string, string) error
	Hardlink(context.Context, string, string) error
}

func (d *Driver) publisher(ctx context.Context, mode vfs.PublishMode) (remote.FilePublisher, error) {
	var evidence string
	err := d.publishSession(ctx, func(s PublishSession) error {
		version, link := s.PublishExtensions()
		if version != 3 || (mode == vfs.PublishStrict && link != "1") {
			return fmt.Errorf("%w: SFTP strict requires v3 hardlink@openssh.com version 1", vfs.ErrWriteUnsupported)
		}
		evidence = strconv.FormatUint(uint64(version), 10) + ":hardlink@openssh.com=" + link
		return nil
	})
	if err != nil {
		return remote.FilePublisher{}, err
	}
	p := remote.FilePublisher{Key: d.Key, Protocol: d.Scheme(), Evidence: evidence, Stat: d.Stat}
	p.Create = func(ctx context.Context, u vfs.URI) (remote.FileUpload, vfs.ObjectIdentity, error) {
		pool, err := d.pool()
		if err != nil {
			return nil, vfs.ObjectIdentity{}, err
		}
		s, err := pool.Acquire(ctx)
		if err != nil {
			return nil, vfs.ObjectIdentity{}, err
		}
		ws, ok := s.(PublishSession)
		if !ok {
			pool.Release(s, false)
			return nil, vfs.ObjectIdentity{}, vfs.ErrWriteUnsupported
		}
		v, l := ws.PublishExtensions()
		if strconv.FormatUint(uint64(v), 10)+":hardlink@openssh.com="+l != evidence {
			pool.Release(s, false)
			return nil, vfs.ObjectIdentity{}, vfs.ErrInvalidSealedObject
		}
		if err = ws.CreateExclusive(ctx, uriPath(u)); err != nil {
			pool.Release(s, true)
			return nil, vfs.ObjectIdentity{}, err
		}
		// Creation success proves ownership even when the optional Stat fails.
		return &publishUpload{pool: pool, session: ws, path: uriPath(u)}, vfs.ObjectIdentity{}, nil
	}
	p.Read = func(ctx context.Context, u vfs.URI, id vfs.ObjectIdentity, w io.Writer, b []byte) error {
		return d.publishSession(ctx, func(s PublishSession) error {
			var off int64
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				n, err := s.ReadAt(ctx, uriPath(u), b, off)
				if n > 0 {
					if _, e := w.Write(b[:n]); e != nil {
						return e
					}
					off += int64(n)
				}
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				if n == 0 {
					return io.ErrNoProgress
				}
			}
		})
	}
	p.Move = func(ctx context.Context, source, dest vfs.URI, mode vfs.PublishMode, _ vfs.ObjectIdentity) error {
		return d.publishSession(ctx, func(s PublishSession) error {
			version, l := s.PublishExtensions()
			if strconv.FormatUint(uint64(version), 10)+":hardlink@openssh.com="+l != evidence || mode == vfs.PublishStrict && l != "1" {
				return vfs.ErrPublishFailed
			}
			if _, err := s.Stat(ctx, uriPath(dest)); err == nil {
				return vfs.ErrPublishConflict
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			var err error
			if mode == vfs.PublishStrict {
				err = s.Hardlink(ctx, uriPath(source), uriPath(dest))
			} else {
				err = s.Rename(ctx, uriPath(source), uriPath(dest))
			}
			if err != nil && !IsRetryable(err) && ctx.Err() == nil {
				// STATUS failure is a definite rejection; v3 uses generic FAILURE
				// for EEXIST, so check the destination to classify a racing create.
				var status *publishStatusError
				if errors.As(err, &status) {
					if _, e := s.Stat(ctx, uriPath(dest)); e == nil {
						return errors.Join(vfs.ErrPublishConflict, err)
					}
					return errors.Join(vfs.ErrPublishFailed, err)
				}
			}
			return err
		})
	}
	p.Delete = func(ctx context.Context, u vfs.URI, _ vfs.ObjectIdentity) error {
		return d.publishSession(ctx, func(s PublishSession) error {
			err := s.Remove(ctx, uriPath(u))
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		})
	}
	return p, nil
}

// Mutations are never passed through withSession's retry loop.
func (d *Driver) publishSession(ctx context.Context, fn func(PublishSession) error) error {
	p, err := d.pool()
	if err != nil {
		return err
	}
	s, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	ws, ok := s.(PublishSession)
	if !ok {
		p.Release(s, false)
		return vfs.ErrWriteUnsupported
	}
	err = fn(ws)
	p.Release(s, err != nil || ctx.Err() != nil)
	return err
}

func (d *Driver) PlanPublish(ctx context.Context, u vfs.URI, job string, mode vfs.PublishMode) (vfs.PublishIntent, error) {
	if err := d.validateURI(u); err != nil {
		return vfs.PublishIntent{}, err
	}
	p, err := d.publisher(ctx, mode)
	if err != nil {
		return vfs.PublishIntent{}, err
	}
	return p.Plan(u, job, mode)
}
func (d *Driver) BeginPublish(ctx context.Context, i vfs.PublishIntent, r vfs.RecordOwnership, s *memory.Scope, b *memory.BufferManager) (vfs.PublishHandle, error) {
	p, e := d.publisher(ctx, i.Mode)
	if e != nil {
		return nil, e
	}
	return p.Begin(ctx, i, r, s, b)
}
func (d *Driver) ResumePublish(ctx context.Context, i vfs.PublishIntent, o []byte, z *vfs.SealedObject, s *memory.Scope, b *memory.BufferManager) (vfs.PublishHandle, error) {
	p, e := d.publisher(ctx, i.Mode)
	if e != nil {
		return nil, e
	}
	return p.Resume(ctx, i, o, z, s, b)
}

type publishUpload struct {
	pool    *Pool
	session PublishSession
	path    string
	off     int64
}

func (u *publishUpload) Write(ctx context.Context, b []byte) (int, error) {
	if u.session == nil {
		return 0, vfs.ErrWriteClosed
	}
	n, e := u.session.WriteExisting(ctx, u.path, b, u.off)
	u.off += int64(n)
	return n, e
}
func (u *publishUpload) Close(context.Context) error { return u.Release() }
func (u *publishUpload) Release() error {
	if u.session != nil {
		u.pool.Release(u.session, true)
		u.session = nil
	}
	return nil
}
