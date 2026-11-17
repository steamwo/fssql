package ftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type RenameSession interface {
	Rename(context.Context, string, string) error
}
type physicalReader interface {
	ReadTo(context.Context, string, io.Writer, []byte) error
}

func (d *Driver) publisher(ctx context.Context, mode vfs.PublishMode) (remote.FilePublisher, error) {
	if mode != vfs.PublishCompatible {
		return remote.FilePublisher{}, fmt.Errorf("%w: FTP/FTPS requires compatible publish mode", vfs.ErrWriteUnsupported)
	}
	key := d.Key
	if factory, ok := d.Pools.factory.(*WireFactory); ok {
		if factory.Policy == nil || factory.Credentials == nil {
			return remote.FilePublisher{}, vfs.ErrCredential
		}
		if _, err := factory.Policy.ValidateURL(ctx, d.Scheme()+"://"+d.Key.Endpoint+"/"); err != nil {
			return remote.FilePublisher{}, err
		}
		credentials, err := factory.Credentials.Credentials(ctx, d.Key, d.Scheme())
		if err != nil {
			return remote.FilePublisher{}, err
		}
		key.CredentialVersion = remote.PublishScope(key.CredentialVersion, credentials.Username, credentials.Password)
	}
	p := remote.FilePublisher{Key: key, Protocol: d.Scheme(), Evidence: "STOR;existence-check;RNFR/RNTO;TOCTOU", Stat: d.publishStat}
	p.Create = func(ctx context.Context, u vfs.URI) (remote.FileUpload, vfs.ObjectIdentity, error) {
		if _, err := d.publishStat(ctx, u); err == nil {
			return nil, vfs.ObjectIdentity{}, vfs.ErrPublishConflict
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, vfs.ObjectIdentity{}, err
		}
		pool, err := d.pool()
		if err != nil {
			return nil, vfs.ObjectIdentity{}, err
		}
		s, err := pool.Acquire(ctx)
		if err != nil {
			return nil, vfs.ObjectIdentity{}, err
		}
		if _, ok := s.(RenameSession); !ok {
			pool.Release(s, false)
			return nil, vfs.ObjectIdentity{}, vfs.ErrWriteUnsupported
		}
		w, err := s.OpenWrite(ctx, uriPath(u))
		if err != nil {
			pool.Release(s, IsRetryable(err) || ctx.Err() != nil)
			return nil, vfs.ObjectIdentity{}, err
		}
		return &publishUpload{pool: pool, session: s, writer: w}, vfs.ObjectIdentity{}, nil
	}
	p.Read = func(ctx context.Context, u vfs.URI, id vfs.ObjectIdentity, w io.Writer, b []byte) error {
		return d.publishSession(ctx, func(s Session) error {
			if r, ok := s.(physicalReader); ok {
				return r.ReadTo(ctx, uriPath(u), w, b)
			}
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
	p.Move = func(ctx context.Context, source, dest vfs.URI, _ vfs.PublishMode, _ vfs.ObjectIdentity) error {
		if _, err := d.publishStat(ctx, dest); err == nil {
			return vfs.ErrPublishConflict
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return d.publishSession(ctx, func(s Session) error {
			r, ok := s.(RenameSession)
			if !ok {
				return vfs.ErrPublishFailed
			}
			return r.Rename(ctx, uriPath(source), uriPath(dest))
		})
	}
	p.Delete = func(ctx context.Context, u vfs.URI, _ vfs.ObjectIdentity) error {
		return d.publishSession(ctx, func(s Session) error { return s.Delete(ctx, uriPath(u)) })
	}
	return p, nil
}

// FTP 550 also means permission denied. Only an exact parent MLSD result can
// prove absence; never reinterpret arbitrary SIZE errors as a missing target.
func (d *Driver) publishStat(ctx context.Context, u vfs.URI) (vfs.ObjectInfo, error) {
	info, err := d.Stat(ctx, u)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return info, err
	}
	var status *statusError
	if !errors.As(err, &status) || status.code != 550 {
		return info, err
	}
	listErr := d.publishSession(ctx, func(s Session) error {
		entries, e := s.List(ctx, path.Dir(uriPath(u)))
		if e != nil {
			return e
		}
		for _, entry := range entries {
			if entry.Name == path.Base(uriPath(u)) {
				return err
			}
		}
		return os.ErrNotExist
	})
	if errors.Is(listErr, os.ErrNotExist) {
		return vfs.ObjectInfo{}, os.ErrNotExist
	}
	return info, err
}

func (d *Driver) publishSession(ctx context.Context, fn func(Session) error) error {
	p, err := d.pool()
	if err != nil {
		return err
	}
	s, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	err = fn(s)
	p.Release(s, IsRetryable(err) || ctx.Err() != nil)
	return err
}
func (d *Driver) PlanPublish(ctx context.Context, u vfs.URI, job string, mode vfs.PublishMode) (vfs.PublishIntent, error) {
	if err := d.validateURI(u); err != nil {
		return vfs.PublishIntent{}, err
	}
	p, e := d.publisher(ctx, mode)
	if e != nil {
		return vfs.PublishIntent{}, e
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
	session Session
	writer  StreamWriter
	closed  bool
}

func (u *publishUpload) Write(ctx context.Context, b []byte) (int, error) {
	if u.writer == nil {
		return 0, vfs.ErrWriteClosed
	}
	return u.writer.Write(ctx, b)
}
func (u *publishUpload) Close(ctx context.Context) error {
	if u.writer == nil {
		return vfs.ErrWriteClosed
	}
	err := u.writer.Close(ctx)
	if err == nil {
		u.closed = true
	}
	return err
}
func (u *publishUpload) Release() error {
	if u.session == nil {
		return nil
	}
	var err error
	if !u.closed {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = u.writer.Abort(ctx)
		cancel()
	}
	u.pool.Release(u.session, !u.closed)
	u.session = nil
	u.writer = nil
	return err
}
