package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/steamwo/fssql/internal/resource"
)

const shutdownPollInterval = 2 * time.Millisecond

// ShutdownReport 描述一次 QueryService graceful shutdown 的最终清理结果。
type ShutdownReport struct {
	// CanceledQueries 是 shutdown 主动请求取消的已 admission Query 数。
	CanceledQueries int
	// ForgottenQueries 是 shutdown 已从 Registry/runtime retention 删除的终态 Query 数。
	ForgottenQueries int
	// RemainingQueries 是 shutdown 返回时仍未完成 lifecycle cleanup 的 Query ID；成功时必须为空。
	RemainingQueries []QueryID
}

// Shutdown 停止新 admission、取消并等待所有已 admission Query 完成 lifecycle cleanup，随后释放 terminal retention。
// 它不创建第二套 Query 生命周期；所有取消/终态发布仍复用 QueryHandle、QueryContext 与 ResultCursor。
func (s *QueryService) Shutdown(ctx context.Context) (ShutdownReport, error) {
	if s == nil {
		return ShutdownReport{}, errors.New("server: nil query service")
	}
	if ctx == nil {
		return ShutdownReport{}, errors.New("server: shutdown context required")
	}
	if err := ctx.Err(); err != nil {
		return ShutdownReport{}, err
	}

	// 先关闭 AD-016 admission；SUBMITTED/QUEUED Query 会由 Scheduler.Close 唤醒并走既有 rejected-submit cleanup。
	s.resources.Close()
	report := ShutdownReport{}
	canceled := make(map[QueryID]struct{})
	forgotten := make(map[QueryID]struct{})
	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()

	for {
		ids := s.shutdownRuntimeIDs()
		for _, id := range ids {
			snapshot, err := s.Status(id)
			if err != nil {
				if errors.Is(err, ErrQueryNotFound) {
					continue
				}
				return report, err
			}
			if !snapshot.State.Terminal() {
				// 未 admission 的 Query 必须让 Scheduler.Close 负责失败唤醒，避免与 Submit 的 rejected-cleanup 竞争终态 ownership。
				if snapshot.State == QuerySubmitted || snapshot.State == QueryQueued {
					continue
				}
				if _, already := canceled[id]; !already {
					if err := s.Cancel(id); err != nil && !errors.Is(err, ErrQueryNotFound) {
						return report, err
					}
					canceled[id] = struct{}{}
					report.CanceledQueries++
				}
				continue
			}
			if _, already := forgotten[id]; already {
				continue
			}
			if err := s.Forget(id); err != nil {
				if errors.Is(err, ErrQueryNotFound) {
					continue
				}
				// 终态发布与 lease release 存在极短窗口；Forget 只在真正 terminal 时成功，其他错误必须显式返回。
				return report, err
			}
			forgotten[id] = struct{}{}
			report.ForgottenQueries++
		}

		remaining := s.shutdownRuntimeIDs()
		usage := s.resources.Snapshot().GlobalUsed
		if len(remaining) == 0 && usage == (resource.Usage{}) {
			report.RemainingQueries = nil
			return report, nil
		}

		select {
		case <-ctx.Done():
			report.RemainingQueries = append([]QueryID(nil), remaining...)
			return report, fmt.Errorf("server: graceful shutdown incomplete: remaining=%d resources=%+v: %w", len(remaining), usage, ctx.Err())
		case <-ticker.C:
		}
	}
}

// shutdownRuntimeIDs 返回稳定排序的 runtime ID 快照，避免在持锁期间执行 Cancel/Forget。
func (s *QueryService) shutdownRuntimeIDs() []QueryID {
	s.runtimesMu.RLock()
	ids := make([]QueryID, 0, len(s.runtimes))
	for id := range s.runtimes {
		ids = append(ids, id)
	}
	s.runtimesMu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
