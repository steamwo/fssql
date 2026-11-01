package resource

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// testBudget 创建同时约束所有 AD-016 admission 资源维度的测试预算。
func testBudget(concurrent int64) Budget {
	return Budget{
		ConcurrentQueries: concurrent,
		CPUSlots:          8,
		MemoryBytes:       1024,
		RemoteIOSlots:     8,
		SpillIOSlots:      8,
	}
}

// testRequest 创建一个最小合法 admission 请求。
func testRequest(id, group string) Request {
	return Request{
		QueryID: id, Group: group, Class: QueryClassInteractive,
		CPUSlots: 1, MemoryBytes: 64, RemoteIOSlots: 1, SpillIOSlots: 1,
	}
}

func TestSchedulerAccountsAndReleasesAllResourceDimensions(t *testing.T) {
	scheduler, err := NewScheduler(testBudget(2), []ResourceGroup{{Name: "default", Weight: 1, Budget: testBudget(2)}})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest("q1", "default")
	request.CPUSlots = 2
	request.MemoryBytes = 128
	request.RemoteIOSlots = 3
	request.SpillIOSlots = 2
	lease, err := scheduler.Acquire(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := scheduler.Snapshot()
	if snapshot.GlobalUsed.ConcurrentQueries != 1 || snapshot.GlobalUsed.CPUSlots != 2 || snapshot.GlobalUsed.MemoryBytes != 128 || snapshot.GlobalUsed.RemoteIOSlots != 3 || snapshot.GlobalUsed.SpillIOSlots != 2 {
		t.Fatalf("global usage=%+v", snapshot.GlobalUsed)
	}
	if snapshot.Groups["default"].Used.InteractiveQueries != 1 {
		t.Fatalf("group usage=%+v", snapshot.Groups["default"].Used)
	}
	lease.Release()
	lease.Release()
	snapshot = scheduler.Snapshot()
	if snapshot.GlobalUsed != (Usage{}) || snapshot.Groups["default"].Used != (Usage{}) || !lease.Released() {
		t.Fatalf("usage after release: global=%+v group=%+v released=%v", snapshot.GlobalUsed, snapshot.Groups["default"].Used, lease.Released())
	}
}

func TestSchedulerRejectsUnknownGroupAndOversizedRequest(t *testing.T) {
	global := testBudget(2)
	groupBudget := testBudget(2)
	groupBudget.MemoryBytes = 100
	scheduler, err := NewScheduler(global, []ResourceGroup{{Name: "small", Weight: 1, Budget: groupBudget}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Acquire(context.Background(), testRequest("q1", "missing")); !errors.Is(err, ErrUnknownGroup) {
		t.Fatalf("unknown group err=%v", err)
	}
	request := testRequest("q2", "small")
	request.MemoryBytes = 101
	if _, err := scheduler.Acquire(context.Background(), request); !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("oversized err=%v", err)
	}
}

func TestQueuedCancellationRemovesWaiterWithoutResourceLeak(t *testing.T) {
	scheduler, err := NewScheduler(testBudget(1), []ResourceGroup{{Name: "default", Weight: 1, Budget: testBudget(1)}})
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := scheduler.Acquire(context.Background(), testRequest("blocker", "default"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, acquireErr := scheduler.Acquire(ctx, testRequest("queued", "default"))
		result <- acquireErr
	}()
	waitForQueued(t, scheduler, "default", 1)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation err=%v", err)
	}
	snapshot := scheduler.Snapshot()
	if snapshot.Groups["default"].QueuedQueries != 0 || snapshot.GlobalUsed.ConcurrentQueries != 1 {
		t.Fatalf("snapshot after cancel=%+v", snapshot)
	}
	blocker.Release()
	if scheduler.Snapshot().GlobalUsed != (Usage{}) {
		t.Fatalf("global usage leaked: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

func TestEqualWeightGroupsDoNotStarve(t *testing.T) {
	global := testBudget(1)
	groups := []ResourceGroup{
		{Name: "a", Weight: 1, Budget: testBudget(1)},
		{Name: "b", Weight: 1, Budget: testBudget(1)},
		{Name: "block", Weight: 1, Budget: testBudget(1)},
	}
	scheduler, err := NewScheduler(global, groups)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := scheduler.Acquire(context.Background(), testRequest("blocker", "block"))
	if err != nil {
		t.Fatal(err)
	}

	type admitted struct {
		group string
		lease *Lease
		err   error
	}
	results := make(chan admitted, 6)
	for i := 0; i < 3; i++ {
		for _, group := range []string{"a", "b"} {
			request := testRequest(fmt.Sprintf("%s-%d", group, i), group)
			go func(group string, request Request) {
				lease, acquireErr := scheduler.Acquire(context.Background(), request)
				results <- admitted{group: group, lease: lease, err: acquireErr}
			}(group, request)
		}
	}
	waitForQueued(t, scheduler, "a", 3)
	waitForQueued(t, scheduler, "b", 3)
	blocker.Release()

	order := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		order = append(order, result.group)
		result.lease.Release()
	}
	for i := 1; i < len(order); i++ {
		if order[i] == order[i-1] {
			t.Fatalf("equal-weight groups were not alternated: %v", order)
		}
	}
}

func TestWeightedFairnessFollowsSmoothWRRWithoutStarvation(t *testing.T) {
	global := testBudget(1)
	groups := []ResourceGroup{
		{Name: "a", Weight: 2, Budget: testBudget(1)},
		{Name: "b", Weight: 1, Budget: testBudget(1)},
		{Name: "block", Weight: 1, Budget: testBudget(1)},
	}
	scheduler, err := NewScheduler(global, groups)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := scheduler.Acquire(context.Background(), testRequest("blocker", "block"))
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		group string
		lease *Lease
		err   error
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan result, 20)
	for i := 0; i < 10; i++ {
		for _, group := range []string{"a", "b"} {
			request := testRequest(fmt.Sprintf("%s-%d", group, i), group)
			go func(group string, request Request) {
				lease, acquireErr := scheduler.Acquire(ctx, request)
				results <- result{group: group, lease: lease, err: acquireErr}
			}(group, request)
		}
	}
	waitForQueued(t, scheduler, "a", 10)
	waitForQueued(t, scheduler, "b", 10)
	blocker.Release()

	counts := map[string]int{}
	var lastLease *Lease
	for i := 0; i < 6; i++ {
		admission := <-results
		if admission.err != nil {
			t.Fatal(admission.err)
		}
		counts[admission.group]++
		if i < 5 {
			admission.lease.Release()
		} else {
			lastLease = admission.lease
		}
	}
	if counts["a"] != 4 || counts["b"] != 2 {
		t.Fatalf("weighted first six admissions=%v, want a=4 b=2", counts)
	}

	// 第六个 Lease 保持 active，先取消其余排队请求，避免取消之后再产生新的 admission。
	cancel()
	canceled := 0
	for canceled < 14 {
		pending := <-results
		if pending.lease != nil {
			pending.lease.Release()
			t.Fatalf("unexpected admission after cancellation: group=%s", pending.group)
		}
		if !errors.Is(pending.err, context.Canceled) {
			t.Fatalf("queued err=%v, want context.Canceled", pending.err)
		}
		canceled++
	}
	lastLease.Release()
	if scheduler.Snapshot().GlobalUsed != (Usage{}) {
		t.Fatalf("usage leaked after weighted test: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

func TestSchedulerCloseCancelsQueuedButKeepsActiveOwnership(t *testing.T) {
	scheduler, err := NewScheduler(testBudget(1), []ResourceGroup{{Name: "default", Weight: 1, Budget: testBudget(1)}})
	if err != nil {
		t.Fatal(err)
	}
	active, err := scheduler.Acquire(context.Background(), testRequest("active", "default"))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, acquireErr := scheduler.Acquire(context.Background(), testRequest("queued", "default"))
		result <- acquireErr
	}()
	waitForQueued(t, scheduler, "default", 1)
	scheduler.Close()
	if err := <-result; !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("queued close err=%v, want ErrSchedulerClosed", err)
	}
	snapshot := scheduler.Snapshot()
	if !snapshot.Closed || snapshot.GlobalUsed.ConcurrentQueries != 1 || snapshot.Groups["default"].QueuedQueries != 0 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if _, err := scheduler.Acquire(context.Background(), testRequest("new", "default")); !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("new acquire err=%v, want ErrSchedulerClosed", err)
	}
	active.Release()
	if scheduler.Snapshot().GlobalUsed != (Usage{}) {
		t.Fatalf("active lease did not release after Close: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

// waitForQueued 等待指定 ResourceGroup 达到预期排队长度，避免并发测试依赖固定 sleep 顺序。
func waitForQueued(t *testing.T, scheduler *Scheduler, group string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if scheduler.Snapshot().Groups[group].QueuedQueries == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("group %s queued=%d, want %d", group, scheduler.Snapshot().Groups[group].QueuedQueries, want)
}
