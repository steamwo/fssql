package resource

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// waiterState 描述 admission waiter 的内部生命周期。
type waiterState uint8

const (
	// waiterQueued 表示请求仍在 ResourceGroup FIFO 中。
	waiterQueued waiterState = iota
	// waiterAdmitted 表示资源已记账并生成 Lease。
	waiterAdmitted
	// waiterCanceled 表示排队请求已取消或 scheduler 已关闭。
	waiterCanceled
)

// admissionResult 是 bounded waiter channel 中唯一的 admission 结果。
type admissionResult struct {
	// lease 非 nil 表示 admission 成功。
	lease *Lease
	// err 非 nil 表示请求在 admission 前失败。
	err error
}

// waiter 保存一个排队中的 Query admission 请求。
type waiter struct {
	// request 是不可变 admission 请求。
	request Request
	// state 仅在 Scheduler.mu 下访问。
	state waiterState
	// result 容量固定为 1，dispatch 不会因调用方调度延迟而阻塞。
	result chan admissionResult
	// lease 在 admitted 后指向资源 ownership。
	lease *Lease
}

// groupState 保存单 ResourceGroup 的队列、使用量与 smooth-WRR 状态。
type groupState struct {
	// config 是创建时验证后的不可变配置。
	config ResourceGroup
	// used 是当前 admitted Query 资源使用量。
	used Usage
	// queue 是组内严格 FIFO admission queue。
	queue []*waiter
	// currentWeight 是 smooth weighted round-robin 的运行权重。
	currentWeight int64
}

// Scheduler 是 AD-016 Global Resource Scheduler 的 admission/accounting 层。
// 它不执行 operator Task；CPU Task 仍由 execution/scheduler 调度。
type Scheduler struct {
	// mu 保护 groups/order/globalUsed/closed 和所有 Lease 记账。
	mu sync.Mutex
	// globalBudget 是所有 ResourceGroup 共享的总预算。
	globalBudget Budget
	// globalUsed 是所有 active Lease 的聚合占用。
	globalUsed Usage
	// groups 按稳定名称索引 ResourceGroup 状态。
	groups map[string]*groupState
	// order 是按名称排序的稳定组顺序，用于 WRR tie-break。
	order []string
	// closed 阻止新的 admission，并取消所有仍排队的 waiter。
	closed bool
}

// Lease 表示一个 Query 已获得的全局与 ResourceGroup 资源 ownership。
type Lease struct {
	// scheduler 是资源记账 owner。
	scheduler *Scheduler
	// request 保存该 Query 的资源占用量。
	request Request
	// released 仅在 scheduler.mu 下访问，保证 Release 幂等。
	released bool
}

// NewScheduler 创建 Global Resource Scheduler，并固定 ResourceGroup 集合。
func NewScheduler(global Budget, groups []ResourceGroup) (*Scheduler, error) {
	if err := global.Validate(); err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("%w: at least one resource group required", ErrInvalidBudget)
	}
	s := &Scheduler{globalBudget: global, groups: make(map[string]*groupState, len(groups))}
	for _, config := range groups {
		if err := config.Validate(); err != nil {
			return nil, err
		}
		if _, exists := s.groups[config.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate resource group %s", ErrInvalidBudget, config.Name)
		}
		s.groups[config.Name] = &groupState{config: config}
		s.order = append(s.order, config.Name)
	}
	sort.Strings(s.order)
	return s, nil
}

// Acquire 把 Query 放入 ResourceGroup FIFO，并在公平调度获得全部预算后返回 Lease。
func (s *Scheduler) Acquire(ctx context.Context, request Request) (*Lease, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSchedulerClosed
	}
	group := s.groups[request.Group]
	if group == nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrUnknownGroup, request.Group)
	}
	if !requestFitsBudget(request, s.globalBudget) {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: query=%s global", ErrRequestTooLarge, request.QueryID)
	}
	if !requestFitsBudget(request, group.config.Budget) {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: query=%s group=%s", ErrRequestTooLarge, request.QueryID, request.Group)
	}
	entry := &waiter{request: request, state: waiterQueued, result: make(chan admissionResult, 1)}
	group.queue = append(group.queue, entry)
	s.dispatchLocked()
	s.mu.Unlock()

	select {
	case result := <-entry.result:
		if result.err != nil {
			return nil, result.err
		}
		if err := ctx.Err(); err != nil {
			result.lease.Release()
			return nil, err
		}
		return result.lease, nil
	case <-ctx.Done():
		var admitted *Lease
		s.mu.Lock()
		switch entry.state {
		case waiterQueued:
			s.removeWaiterLocked(group, entry)
			entry.state = waiterCanceled
			s.dispatchLocked()
		case waiterAdmitted:
			admitted = entry.lease
		}
		s.mu.Unlock()
		if admitted != nil {
			admitted.Release()
		}
		return nil, ctx.Err()
	}
}

// Release 归还 Lease 占用的全部资源并触发下一轮公平 admission；可重复调用。
func (l *Lease) Release() {
	if l == nil || l.scheduler == nil {
		return
	}
	s := l.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	group := s.groups[l.request.Group]
	if group != nil {
		subUsage(&group.used, l.request)
	}
	subUsage(&s.globalUsed, l.request)
	s.dispatchLocked()
}

// Request 返回 Lease 对应的不可变 admission 请求副本。
func (l *Lease) Request() Request {
	if l == nil {
		return Request{}
	}
	return l.request
}

// Released 返回 Lease 是否已经归还资源。
func (l *Lease) Released() bool {
	if l == nil || l.scheduler == nil {
		return true
	}
	l.scheduler.mu.Lock()
	defer l.scheduler.mu.Unlock()
	return l.released
}

// Close 停止新 admission 并取消所有仍排队的 waiter；active Lease 继续由 owner 显式 Release。
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, name := range s.order {
		group := s.groups[name]
		for _, entry := range group.queue {
			if entry.state != waiterQueued {
				continue
			}
			entry.state = waiterCanceled
			entry.result <- admissionResult{err: ErrSchedulerClosed}
		}
		group.queue = nil
	}
}

// Snapshot 返回调度器当前资源与排队情况的独立快照。
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	groups := make(map[string]GroupSnapshot, len(s.groups))
	for name, state := range s.groups {
		groups[name] = GroupSnapshot{Config: state.config, Used: state.used, QueuedQueries: len(state.queue)}
	}
	return Snapshot{
		Closed: s.closed, GlobalBudget: s.globalBudget, GlobalUsed: s.globalUsed, Groups: groups,
	}
}

// dispatchLocked 持续选择当前可运行组，直到没有任何队首请求能同时满足全局和组预算。
func (s *Scheduler) dispatchLocked() {
	if s.closed {
		return
	}
	for {
		group := s.selectGroupLocked()
		if group == nil {
			return
		}
		entry := group.queue[0]
		group.queue = group.queue[1:]
		lease := &Lease{scheduler: s, request: entry.request}
		entry.state = waiterAdmitted
		entry.lease = lease
		addUsage(&group.used, entry.request)
		addUsage(&s.globalUsed, entry.request)
		entry.result <- admissionResult{lease: lease}
	}
}

// selectGroupLocked 使用 smooth weighted round-robin 在当前“队首可满足”的组之间选择下一组。
// 队首暂时不满足预算时不会绕过本组 FIFO，但也不会阻塞其他 ResourceGroup。
func (s *Scheduler) selectGroupLocked() *groupState {
	eligible := make([]*groupState, 0, len(s.order))
	var totalWeight int64
	for _, name := range s.order {
		group := s.groups[name]
		if len(group.queue) == 0 {
			continue
		}
		request := group.queue[0].request
		if !fitsAvailable(s.globalUsed, request, s.globalBudget) || !fitsAvailable(group.used, request, group.config.Budget) {
			continue
		}
		group.currentWeight += group.config.Weight
		totalWeight += group.config.Weight
		eligible = append(eligible, group)
	}
	if len(eligible) == 0 {
		return nil
	}
	selected := eligible[0]
	for _, candidate := range eligible[1:] {
		if candidate.currentWeight > selected.currentWeight {
			selected = candidate
		}
	}
	selected.currentWeight -= totalWeight
	return selected
}

// removeWaiterLocked 从组内 FIFO 移除已取消 waiter；只在 cancellation 慢路径使用。
func (s *Scheduler) removeWaiterLocked(group *groupState, target *waiter) {
	for index, entry := range group.queue {
		if entry != target {
			continue
		}
		copy(group.queue[index:], group.queue[index+1:])
		group.queue[len(group.queue)-1] = nil
		group.queue = group.queue[:len(group.queue)-1]
		return
	}
}

// requestFitsBudget 判断单个 Query 是否理论上能被给定预算容纳。
func requestFitsBudget(request Request, budget Budget) bool {
	return withinLimit(1, budget.ConcurrentQueries) &&
		withinLimit(request.CPUSlots, budget.CPUSlots) &&
		withinLimit(request.MemoryBytes, budget.MemoryBytes) &&
		withinLimit(request.RemoteIOSlots, budget.RemoteIOSlots) &&
		withinLimit(request.SpillIOSlots, budget.SpillIOSlots)
}

// fitsAvailable 判断加入 request 后 usage 是否仍位于预算内。
func fitsAvailable(usage Usage, request Request, budget Budget) bool {
	return withinLimit(usage.ConcurrentQueries+1, budget.ConcurrentQueries) &&
		withinLimit(usage.CPUSlots+request.CPUSlots, budget.CPUSlots) &&
		withinLimit(usage.MemoryBytes+request.MemoryBytes, budget.MemoryBytes) &&
		withinLimit(usage.RemoteIOSlots+request.RemoteIOSlots, budget.RemoteIOSlots) &&
		withinLimit(usage.SpillIOSlots+request.SpillIOSlots, budget.SpillIOSlots)
}

// withinLimit 判断 value 是否满足有限预算或 Unlimited。
func withinLimit(value, limit int64) bool {
	return limit == Unlimited || value <= limit
}

// addUsage 记录 Query admission 的资源 ownership。
func addUsage(usage *Usage, request Request) {
	usage.ConcurrentQueries++
	if request.Class == QueryClassInteractive {
		usage.InteractiveQueries++
	} else if request.Class == QueryClassBatch {
		usage.BatchQueries++
	}
	usage.CPUSlots += request.CPUSlots
	usage.MemoryBytes += request.MemoryBytes
	usage.RemoteIOSlots += request.RemoteIOSlots
	usage.SpillIOSlots += request.SpillIOSlots
}

// subUsage 释放 Query admission 的资源 ownership；内部记账不允许出现负数。
func subUsage(usage *Usage, request Request) {
	usage.ConcurrentQueries--
	if request.Class == QueryClassInteractive {
		usage.InteractiveQueries--
	} else if request.Class == QueryClassBatch {
		usage.BatchQueries--
	}
	usage.CPUSlots -= request.CPUSlots
	usage.MemoryBytes -= request.MemoryBytes
	usage.RemoteIOSlots -= request.RemoteIOSlots
	usage.SpillIOSlots -= request.SpillIOSlots
}

// compile-time 使用，防止 errors import 被后续重构意外移除后掩盖 Close path 的错误语义。
var _ = errors.Is
