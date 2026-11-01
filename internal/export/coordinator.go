package export

import (
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/query"
)

var ErrSourceSnapshotMismatch = errors.New("export: source snapshot mismatch")

// Coordinator 负责把 Query 生命周期、Manifest 状态和后续 Writer/Committer 编排到同一导出作业中。
type Coordinator struct {
	// query 是导出拥有的 QueryContext；SourceSnapshot 必须在开始写入前已经冻结。
	query *query.Context
	// manifest 是本次作业唯一可恢复的持久化状态模型。
	manifest *Manifest
}

// NewCoordinator 创建导出协调器；它不会隐式启动写入，也不会重新绑定源快照。
func NewCoordinator(q *query.Context, manifest *Manifest) (*Coordinator, error) {
	if q == nil {
		return nil, errors.New("export: query context is required")
	}
	if manifest == nil {
		return nil, errors.New("export: manifest is required")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return &Coordinator{query: q, manifest: manifest}, nil
}

// ValidateSourceSnapshot 确保 Manifest 与真正 QueryContext 冻结的是同一个 Statement SourceSnapshot。
func (c *Coordinator) ValidateSourceSnapshot() error {
	if c == nil || c.query == nil || c.manifest == nil {
		return errors.New("export: incomplete coordinator")
	}
	snapshot, ok := c.query.SourceSnapshot()
	if !ok {
		return fmt.Errorf("%w: query has no frozen source snapshot", ErrSourceSnapshotMismatch)
	}
	if snapshot.Fingerprint != c.manifest.SourceSnapshot.Fingerprint {
		return fmt.Errorf("%w: query=%q manifest=%q", ErrSourceSnapshotMismatch, snapshot.Fingerprint, c.manifest.SourceSnapshot.Fingerprint)
	}
	return nil
}

// Start 在源快照验证通过后进入 WRITING；验证失败时绝不产生目标文件。
func (c *Coordinator) Start() error {
	if err := c.ValidateSourceSnapshot(); err != nil {
		return err
	}
	return c.manifest.TransitionExport(ExportStateWriting)
}

// Manifest 返回当前协调器使用的 Manifest；调用者应通过其状态方法更新，不得另建并行状态源。
func (c *Coordinator) Manifest() *Manifest {
	if c == nil {
		return nil
	}
	return c.manifest
}
