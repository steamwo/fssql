package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
)

const (
	// DefaultMaxManifestBytes 限制单个 staging Manifest 的最大持久化体积，避免恢复路径出现无界本地读取。
	DefaultMaxManifestBytes int64 = 64 << 20
)

// ManifestStore 定义 Phase 4 resumable export 的 durable job-state 边界。
// 它只保存 Export Manifest，不保存 query temporary data，也不承担 final dataset commit。
type ManifestStore interface {
	// Save 原子持久化一个已经自洽的 Manifest snapshot。
	Save(context.Context, *Manifest) error
	// Load 读取最近一次成功持久化的完整 Manifest snapshot。
	Load(context.Context) (*Manifest, error)
	// Delete 删除本地 staging Manifest；只应在 committed job 不再需要本地恢复状态后调用。
	Delete(context.Context) error
}

// LocalManifestStore 使用本地文件系统保存单节点 export staging Manifest。
type LocalManifestStore struct {
	// path 是稳定 Manifest 文件路径；每次 Save 通过同目录临时文件原子替换。
	path string
	// maxBytes 是单个持久化 Manifest 的硬上限。
	maxBytes int64
	// scope/buffers 管理序列化 reservation 与恢复读取 buffer。
	scope   *memory.Scope
	buffers *memory.BufferManager
}

// NewLocalManifestStore 创建本地 crash-safe Manifest store；maxBytes<=0 使用默认上限。
func NewLocalManifestStore(path string, maxBytes int64, scope *memory.Scope, buffers *memory.BufferManager) (*LocalManifestStore, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return nil, errors.New("export: local manifest path is required")
	}
	if scope == nil || buffers == nil {
		return nil, errors.New("export: local manifest store requires managed memory")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxManifestBytes
	}
	return &LocalManifestStore{path: path, maxBytes: maxBytes, scope: scope, buffers: buffers}, nil
}

// Save 先同步完整临时文件，再通过平台持久化发布操作替换 snapshot，避免恢复时读取半个 Manifest。
func (s *LocalManifestStore) Save(ctx context.Context, manifest *Manifest) error {
	if s == nil || manifest == nil {
		return errors.New("export: invalid local manifest save")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	if manifest.ManifestVersion == 2 {
		old, err := s.Load(ctx)
		if errors.Is(err, os.ErrNotExist) {
			if manifest.Revision != 1 || manifest.ExportState != ExportStatePlanned {
				return ErrStaleRevision
			}
		} else if err != nil {
			return err
		} else if err := validatePublishUpdate(old, manifest); err != nil {
			return err
		}
	}
	upper, err := manifestJSONUpperBound(manifest)
	if err != nil {
		return err
	}
	reservation, err := s.scope.Reserve(memory.TagMetadata, upper)
	if err != nil {
		return err
	}
	defer reservation.Release()
	payload, err := manifest.MarshalBinary()
	if err != nil {
		return err
	}
	if int64(len(payload)) > s.maxBytes {
		return fmt.Errorf("export: manifest size %d exceeds local store limit %d", len(payload), s.maxBytes)
	}

	dir := filepath.Dir(s.path)
	if err := prepareLocalManifestDir(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	published := false
	defer func() {
		if !published {
			_ = os.Remove(tmpName)
		}
	}()

	if err := writeLocalManifestAll(ctx, tmp, payload); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := publishLocalManifest(ctx, tmpName, s.path); err != nil {
		return err
	}
	published = true
	return nil
}

// Load 通过 Managed Metadata buffer 读取一个原子发布的 Manifest snapshot，并在返回前执行完整 Validate。
func (s *LocalManifestStore) Load(ctx context.Context) (*Manifest, error) {
	if s == nil {
		return nil, errors.New("export: nil local manifest store")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() <= 0 || info.Size() > s.maxBytes {
		return nil, fmt.Errorf("export: invalid local manifest size %d", info.Size())
	}
	maxInt := int64(^uint(0) >> 1)
	if info.Size() > maxInt {
		return nil, fmt.Errorf("export: local manifest too large for platform: %d", info.Size())
	}
	buffer, err := s.buffers.Allocate(s.scope, memory.TagMetadata, int(info.Size()))
	if err != nil {
		return nil, err
	}
	defer buffer.Release()
	if _, err := io.ReadFull(file, buffer.Bytes()); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return UnmarshalManifest(buffer.Bytes())
}

// Delete 通过平台持久化删除操作移除 staging state，缺失路径保持幂等。
func (s *LocalManifestStore) Delete(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return removeLocalManifest(ctx, s.path)
}

// writeLocalManifestAll 完整写出 payload，并在每次文件写入前观察取消状态。
func writeLocalManifestAll(ctx context.Context, file *os.File, payload []byte) error {
	for len(payload) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := file.Write(payload)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}
