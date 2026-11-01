package query

import (
	"errors"
	"fmt"
	"strings"
)

var ErrSourceSnapshotConflict = errors.New("query: source snapshot conflict")

// SourceSnapshot 表示 Statement 在 bind/planning 完成后冻结的全部输入数据身份。
type SourceSnapshot struct {
	// Fingerprint 是由不可变 ScanSet 身份组合得到的稳定指纹。
	Fingerprint string
}

// BindSourceSnapshot 将 Statement 的源快照绑定到 QueryContext；同一上下文只允许幂等绑定同一指纹。
func (q *Context) BindSourceSnapshot(snapshot SourceSnapshot) error {
	if q == nil {
		return errors.New("query: nil context")
	}
	snapshot.Fingerprint = strings.TrimSpace(snapshot.Fingerprint)
	if snapshot.Fingerprint == "" {
		return errors.New("query: empty source snapshot fingerprint")
	}
	copySnapshot := snapshot
	if q.sourceSnapshot.CompareAndSwap(nil, &copySnapshot) {
		return nil
	}
	current := q.sourceSnapshot.Load()
	if current != nil && current.Fingerprint == snapshot.Fingerprint {
		return nil
	}
	return fmt.Errorf("%w: current=%q next=%q", ErrSourceSnapshotConflict, current.Fingerprint, snapshot.Fingerprint)
}

// SourceSnapshot 返回当前冻结快照的副本，调用方不能修改 QueryContext 内保存的身份。
func (q *Context) SourceSnapshot() (SourceSnapshot, bool) {
	if q == nil {
		return SourceSnapshot{}, false
	}
	current := q.sourceSnapshot.Load()
	if current == nil {
		return SourceSnapshot{}, false
	}
	return *current, true
}
