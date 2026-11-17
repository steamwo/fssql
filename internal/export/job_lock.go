package export

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

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
	f, err := os.OpenFile(filepath.Join(abs, job+".lock"), os.O_CREATE|os.O_RDWR, 0600)
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
