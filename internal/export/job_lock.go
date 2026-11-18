package export

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var processJobLocks sync.Map // Canonical lock path to a channel closed at release.

// LockJob uses a persistent lock inode. Never unlink it on unlock. OS locks
// coordinate both threads and processes and are released when a process exits.
func LockJob(ctx context.Context, dir, job string) (func() error, error) {
	if _, err := parseJobID(job); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := prepareLocalManifestDir(abs); err != nil {
		return nil, err
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(abs, job+".lock")
	release, err := lockJobInProcess(ctx, path)
	if err != nil {
		return nil, err
	}
	unlock, err := lockJobFile(ctx, path)
	if err != nil {
		release()
		return nil, err
	}
	var once sync.Once
	var unlockErr error
	return func() error {
		once.Do(func() {
			unlockErr = unlock()
			release()
		})
		return unlockErr
	}, nil
}

// POSIX record locks are process-wide, and closing another descriptor for the
// same file would release them. Serialize access before opening the lock file.
func lockJobInProcess(ctx context.Context, path string) (func(), error) {
	done := make(chan struct{})
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current, busy := processJobLocks.LoadOrStore(path, done)
		if !busy {
			return func() {
				processJobLocks.Delete(path)
				close(done)
			}, nil
		}
		select {
		case <-current.(chan struct{}):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func lockJobFile(ctx context.Context, path string) (func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			return nil, err
		}
		err = tryLockJob(f)
		if err == nil {
			return func() error { return errors.Join(unlockJob(f), f.Close()) }, nil
		}
		if !jobLockBusy(err) {
			_ = f.Close()
			return nil, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
