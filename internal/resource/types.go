package resource

import (
	"errors"
	"fmt"
)

const (
	// Unlimited 表示对应资源预算不设硬上限；其他负数均非法。
	Unlimited int64 = -1
)

var (
	// ErrInvalidBudget 表示 ResourceGroup 或全局资源预算非法。
	ErrInvalidBudget = errors.New("resource: invalid budget")
	// ErrInvalidRequest 表示 admission request 非法。
	ErrInvalidRequest = errors.New("resource: invalid admission request")
	// ErrUnknownGroup 表示请求引用了未注册 ResourceGroup。
	ErrUnknownGroup = errors.New("resource: unknown resource group")
	// ErrRequestTooLarge 表示单 Query 的资源需求永远无法在全局或组预算内满足。
	ErrRequestTooLarge = errors.New("resource: request exceeds budget")
	// ErrSchedulerClosed 表示 Global Resource Scheduler 已停止接受新 admission。
	ErrSchedulerClosed = errors.New("resource: scheduler closed")
)

// QueryClass 是 AD-016 统一资源调度器识别的 Query 类别。
type QueryClass uint8

const (
	// QueryClassInteractive 表示面向交互延迟的 Query。
	QueryClassInteractive QueryClass = iota + 1
	// QueryClassBatch 表示吞吐优先的批处理 Query。
	QueryClassBatch
)

// Valid 判断 QueryClass 是否属于 Phase 5 已知类别。
func (c QueryClass) Valid() bool {
	return c == QueryClassInteractive || c == QueryClassBatch
}

// Budget 描述 Global Resource Scheduler 同时治理的资源维度。
type Budget struct {
	// ConcurrentQueries 是同时持有 admission lease 的 Query 数上限。
	ConcurrentQueries int64
	// CPUSlots 是 admitted Query 可声明的 CPU slot 总额；不替代 execution/scheduler 的 Task 调度。
	CPUSlots int64
	// MemoryBytes 是 Query admission 的 managed-memory 预算总额；实际 allocation 仍必须经过 MemoryManager。
	MemoryBytes int64
	// RemoteIOSlots 是远程 I/O 并发预算。
	RemoteIOSlots int64
	// SpillIOSlots 是 temporary spill I/O 并发预算。
	SpillIOSlots int64
}

// Validate 检查预算字段只能为非负数或 Unlimited。
func (b Budget) Validate() error {
	values := []struct {
		name  string
		value int64
	}{
		{name: "concurrent_queries", value: b.ConcurrentQueries},
		{name: "cpu_slots", value: b.CPUSlots},
		{name: "memory_bytes", value: b.MemoryBytes},
		{name: "remote_io_slots", value: b.RemoteIOSlots},
		{name: "spill_io_slots", value: b.SpillIOSlots},
	}
	for _, item := range values {
		if item.value < Unlimited {
			return fmt.Errorf("%w: %s=%d", ErrInvalidBudget, item.name, item.value)
		}
	}
	return nil
}

// ResourceGroup 定义一个 admission 隔离域及其组间调度权重。
type ResourceGroup struct {
	// Name 是 ResourceGroup 稳定名称。
	Name string
	// Weight 是 smooth weighted round-robin 的正整数权重。
	Weight int64
	// Budget 是该组可使用的资源上限。
	Budget Budget
}

// Validate 检查 ResourceGroup 名称、权重和预算。
func (g ResourceGroup) Validate() error {
	if g.Name == "" {
		return fmt.Errorf("%w: empty resource group name", ErrInvalidBudget)
	}
	if g.Weight <= 0 {
		return fmt.Errorf("%w: group %s weight=%d", ErrInvalidBudget, g.Name, g.Weight)
	}
	return g.Budget.Validate()
}

// Request 是单个 Query 向 Global Resource Scheduler 提交的 admission 需求。
type Request struct {
	// QueryID 是上层 QueryRegistry 提供的稳定 ID；resource 包不依赖 server 包。
	QueryID string
	// Group 是 ResourceGroup 名称。
	Group string
	// Class 是 Query 类别；当前 fairness 以 ResourceGroup 为主，类别被保留在 usage/accounting 中。
	Class QueryClass
	// CPUSlots 是 Query admission 期间占用的 CPU slot 数。
	CPUSlots int64
	// MemoryBytes 是 Query admission 预留的 managed-memory 预算额。
	MemoryBytes int64
	// RemoteIOSlots 是 Query 可占用的远程 I/O slot 数。
	RemoteIOSlots int64
	// SpillIOSlots 是 Query 可占用的 spill I/O slot 数。
	SpillIOSlots int64
}

// Validate 检查 Query admission request 的控制字段与资源需求。
func (r Request) Validate() error {
	if r.QueryID == "" || r.Group == "" || !r.Class.Valid() {
		return fmt.Errorf("%w: query_id=%q group=%q class=%d", ErrInvalidRequest, r.QueryID, r.Group, r.Class)
	}
	if r.CPUSlots < 0 || r.MemoryBytes < 0 || r.RemoteIOSlots < 0 || r.SpillIOSlots < 0 {
		return fmt.Errorf("%w: negative resource requirement", ErrInvalidRequest)
	}
	return nil
}

// Usage 是 admitted Query 当前占用的资源计数。
type Usage struct {
	// ConcurrentQueries 是正在运行的 admitted Query 数。
	ConcurrentQueries int64
	// InteractiveQueries 是其中 interactive Query 数。
	InteractiveQueries int64
	// BatchQueries 是其中 batch Query 数。
	BatchQueries int64
	// CPUSlots 是已占用 CPU slot 数。
	CPUSlots int64
	// MemoryBytes 是已 admission 的 managed-memory 预算额。
	MemoryBytes int64
	// RemoteIOSlots 是已占用远程 I/O slot 数。
	RemoteIOSlots int64
	// SpillIOSlots 是已占用 spill I/O slot 数。
	SpillIOSlots int64
}

// GroupSnapshot 是单 ResourceGroup 的不可变调度快照。
type GroupSnapshot struct {
	// Config 是该组创建时的不可变配置副本。
	Config ResourceGroup
	// Used 是当前 admitted Query 资源占用。
	Used Usage
	// QueuedQueries 是当前 FIFO admission queue 长度。
	QueuedQueries int
}

// Snapshot 是 Global Resource Scheduler 的不可变控制面快照。
type Snapshot struct {
	// Closed 表示 scheduler 是否已停止接受新请求。
	Closed bool
	// GlobalBudget 是全局预算。
	GlobalBudget Budget
	// GlobalUsed 是所有组聚合资源占用。
	GlobalUsed Usage
	// Groups 按名称提供 ResourceGroup 快照；返回值是独立 map。
	Groups map[string]GroupSnapshot
}
