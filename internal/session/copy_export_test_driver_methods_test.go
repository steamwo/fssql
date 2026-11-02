package session

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// OpenWrite satisfies the underlying WritableDriver contract but intentionally
// fails. Stable COPY tests therefore prove that the atomic path never falls
// back to ordinary direct write.
func (d *copyTestAtomicDriver) OpenWrite(context.Context, vfs.URI, vfs.WriteOptions, *memory.Scope, *memory.BufferManager) (vfs.WriteHandle, error) {
	return nil, errors.New("test: ordinary OpenWrite must not be used by Stable COPY")
}

func (d *copyTestAtomicDriver) Delete(context.Context, vfs.URI, *vfs.ObjectIdentity) error {
	return nil
}
