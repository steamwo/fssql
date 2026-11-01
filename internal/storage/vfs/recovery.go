package vfs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
)

var (
	// ErrIncompleteWriteRecoveryUnsupported 表示目标协议不能提供 ADR-0004 要求的跨进程未完成写恢复能力。
	ErrIncompleteWriteRecoveryUnsupported = errors.New("vfs: incomplete write recovery unsupported")
	// ErrIncompleteWriteRecoveryPartial 表示 driver 无错误返回但仍有已发现 ownership 未清理；Resume 必须停止。
	ErrIncompleteWriteRecoveryPartial = errors.New("vfs: incomplete write recovery is partial")
)

// RecoveryReport 描述一次 job-scoped incomplete write recovery 的结果，不暴露协议私有 token。
type RecoveryReport struct {
	// Discovered 是 recovery scope 内发现的未完成远端写 ownership 数量。
	Discovered int
	// Recovered 是本次已成功释放的 ownership 数量。
	Recovered int
}

// Validate 检查 recovery report 自洽，禁止 driver 用非法计数掩盖未清理资源。
func (r RecoveryReport) Validate() error {
	if r.Discovered < 0 || r.Recovered < 0 || r.Recovered > r.Discovered {
		return fmt.Errorf("vfs: invalid recovery report discovered=%d recovered=%d", r.Discovered, r.Recovered)
	}
	return nil
}

// Complete 判断已发现的隐藏 write ownership 是否全部清理。
func (r RecoveryReport) Complete() bool {
	return r.Discovered == r.Recovered
}

// RecoverableWritableDriver 是 WritableDriver 的可选 crash-recovery 扩展。
type RecoverableWritableDriver interface {
	WritableDriver
	// RecoverIncompleteWrites 只允许清理调用方独占的 staging scope；具体协议 token 不得泄漏到上层。
	RecoverIncompleteWrites(context.Context, URI, *memory.Scope) (RecoveryReport, error)
}

// RecoverIncompleteWrites 通过 capability gate 调用 driver 的 job-scoped recovery；失败或部分清理时调用方必须停止 Resume。
func (r *Registry) RecoverIncompleteWrites(ctx context.Context, scopeURI URI, scope *memory.Scope) (RecoveryReport, error) {
	if r == nil {
		return RecoveryReport{}, errors.New("vfs: nil registry")
	}
	if scope == nil {
		return RecoveryReport{}, errors.New("vfs: managed memory scope is required for recovery")
	}
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	if strings.TrimSpace(scopeURI.ObjectKey()) == "" {
		return RecoveryReport{}, errors.New("vfs: incomplete write recovery requires a non-root staging scope")
	}
	d, err := r.DriverFor(scopeURI)
	if err != nil {
		return RecoveryReport{}, err
	}
	rd, ok := d.(RecoverableWritableDriver)
	if !ok {
		return RecoveryReport{}, fmt.Errorf("%w: scheme %s", ErrIncompleteWriteRecoveryUnsupported, scopeURI.Scheme)
	}
	caps, err := r.EffectiveCapabilities(ctx, scopeURI)
	if err != nil {
		return RecoveryReport{}, err
	}
	if !caps.Has(IncompleteWriteRecovery) {
		return RecoveryReport{}, fmt.Errorf("%w: scheme %s does not advertise capability", ErrIncompleteWriteRecoveryUnsupported, scopeURI.Scheme)
	}
	report, err := rd.RecoverIncompleteWrites(ctx, scopeURI, scope)
	if err != nil {
		return RecoveryReport{}, err
	}
	if err := report.Validate(); err != nil {
		return RecoveryReport{}, err
	}
	if !report.Complete() {
		return RecoveryReport{}, fmt.Errorf("%w: discovered=%d recovered=%d", ErrIncompleteWriteRecoveryPartial, report.Discovered, report.Recovered)
	}
	return report, nil
}
