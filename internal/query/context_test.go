package query

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/execution/scheduler"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func TestQueryLifecycleQuiescent(t *testing.T) {
	m := memory.NewManager(1024)
	q := New(context.Background(), m)
	task := scheduler.NewTask(func(context.Context) (scheduler.TaskState, error) { return scheduler.DONE, nil })
	if err := scheduler.New(1).Run(q.Context(), []*scheduler.Task{task}, q); err != nil {
		t.Fatal(err)
	}
	if q.ActiveTasks() != 0 {
		t.Fatal("task leak")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatal("memory leak")
	}
}
func TestQueryCloseDetectsReservationLeak(t *testing.T) {
	m := memory.NewManager(1024)
	q := New(context.Background(), m)
	r, err := q.Memory.Reserve(memory.TagVector, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err == nil {
		t.Fatal("expected leak")
	}
	r.Release()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Memory.Reserve(memory.TagVector, 1); !errors.Is(err, memory.ErrClosed) {
		t.Fatalf("memory scope accepted reservation after successful retry: %v", err)
	}
}

func TestQueryCloseCleansTemporaryStorageOwnership(t *testing.T) {
	m := memory.NewManager(1024)
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 1024, DefaultResourceGroup: 1024, DefaultQuery: 1024})
	storage, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	f, err := tempScope.CreateFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("spill")); err != nil {
		t.Fatal(err)
	}

	q := NewWithTemp(context.Background(), m, tempScope)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := quotas.GlobalUsed(); got != 0 {
		t.Fatalf("temp quota leak: %d", got)
	}
	if got := tempScope.OwnedFiles(); got != 0 {
		t.Fatalf("temp file ownership leak: %d", got)
	}
}

func TestQueryCloseRetriesOnlyFailedCleanup(t *testing.T) {
	q := New(context.Background(), memory.NewManager(1024))
	failure := errors.New("cleanup failed")
	stableCalls := 0
	retryCalls := 0
	if err := q.AddCleanup(func() error { stableCalls++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.AddCleanup(func() error {
		retryCalls++
		if retryCalls == 1 {
			return failure
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); !errors.Is(err, failure) {
		t.Fatalf("first close error = %v, want cleanup failure", err)
	}
	if stableCalls != 1 || retryCalls != 1 {
		t.Fatalf("first close calls = (%d, %d), want (1, 1)", stableCalls, retryCalls)
	}
	if !q.Closed() {
		t.Fatal("query accepted new work after cleanup started")
	}
	if err := q.AddCleanup(func() error { return nil }); err == nil {
		t.Fatal("cleanup registration succeeded after close started")
	}
	if _, err := q.Memory.Reserve(memory.TagVector, 1); !errors.Is(err, memory.ErrClosed) {
		t.Fatalf("successfully closed memory scope was reopened: %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("successful retry returned retained error: %v", err)
	}
	if stableCalls != 1 || retryCalls != 2 {
		t.Fatalf("retry calls = (%d, %d), want (1, 2)", stableCalls, retryCalls)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if stableCalls != 1 || retryCalls != 2 {
		t.Fatalf("completed close repeated cleanup: (%d, %d)", stableCalls, retryCalls)
	}
}

func TestQueryCloseRetainsErrorsAcrossFailedRetries(t *testing.T) {
	q := New(context.Background(), memory.NewManager(1024))
	firstFailure := errors.New("first cleanup failure")
	secondFailure := errors.New("second cleanup failure")
	calls := 0
	if err := q.AddCleanup(func() error {
		calls++
		switch calls {
		case 1:
			return firstFailure
		case 2:
			return secondFailure
		default:
			return nil
		}
	}); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); !errors.Is(err, firstFailure) {
		t.Fatalf("first close error = %v", err)
	}
	err := q.Close()
	if !errors.Is(err, firstFailure) || !errors.Is(err, secondFailure) {
		t.Fatalf("second close error = %v, want both failures", err)
	}
	if calls != 2 {
		t.Fatalf("failed cleanup calls = %d, want 2", calls)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("successful final retry returned error: %v", err)
	}
}

func TestQueryCloseActiveTaskKeepsOriginalEarlyCloseBehavior(t *testing.T) {
	q := New(context.Background(), memory.NewManager(1024))
	q.TaskStarted()
	if err := q.Close(); err == nil {
		t.Fatal("close with active task succeeded")
	}
	if q.Closed() {
		t.Fatal("active-task early close marked query closed")
	}
	if q.Context().Err() == nil {
		t.Fatal("active-task early close did not cancel query context")
	}
	calls := 0
	if err := q.AddCleanup(func() error { calls++; return nil }); err != nil {
		t.Fatalf("active-task early close changed cleanup registration behavior: %v", err)
	}
	q.TaskFinished()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", calls)
	}
}

func TestQueryCloseConcurrentCallsWaitAndRetry(t *testing.T) {
	q := New(context.Background(), memory.NewManager(1024))
	firstFailure := errors.New("concurrent first failure")
	secondFailure := errors.New("concurrent second failure")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	if err := q.AddCleanup(func() error {
		switch calls.Add(1) {
		case 1:
			close(firstStarted)
			<-releaseFirst
			return firstFailure
		case 2:
			return secondFailure
		default:
			return nil
		}
	}); err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- q.Close() }()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first close did not start cleanup")
	}
	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- q.Close()
	}()
	<-secondStarted

	var secondErr error
	secondReturned := false
	select {
	case secondErr = <-secondDone:
		secondReturned = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; !errors.Is(err, firstFailure) {
		t.Fatalf("first close error = %v", err)
	}
	if secondReturned {
		t.Fatalf("concurrent close returned before first attempt finished: %v", secondErr)
	}
	secondErr = <-secondDone
	if !errors.Is(secondErr, firstFailure) || !errors.Is(secondErr, secondFailure) {
		t.Fatalf("concurrent retry error = %v, want both failures", secondErr)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("concurrent cleanup calls = %d, want 2", got)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("successful retry after concurrent failures returned error: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("final cleanup calls = %d, want 3", got)
	}
}

func TestQueryAddCleanupRejectedWhileCloseIsRunning(t *testing.T) {
	q := New(context.Background(), memory.NewManager(1024))
	started := make(chan struct{})
	release := make(chan struct{})
	if err := q.AddCleanup(func() error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- q.Close() }()
	<-started

	lateCalls := 0
	registered := make(chan error, 1)
	go func() { registered <- q.AddCleanup(func() error { lateCalls++; return nil }) }()
	select {
	case err := <-registered:
		if err == nil {
			t.Fatal("late cleanup registration succeeded")
		}
	case <-time.After(time.Second):
		close(release)
		<-closeDone
		t.Fatal("late cleanup registration blocked behind running cleanup")
	}
	close(release)
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if lateCalls != 0 {
		t.Fatal("rejected late cleanup was executed")
	}
}

func TestQueryAddCleanupCloseRaceIsLinearized(t *testing.T) {
	for i := 0; i < 50; i++ {
		q := New(context.Background(), memory.NewManager(1024))
		var calls atomic.Int32
		start := make(chan struct{})
		addDone := make(chan error, 1)
		closeDone := make(chan error, 1)
		go func() {
			<-start
			addDone <- q.AddCleanup(func() error { calls.Add(1); return nil })
		}()
		go func() {
			<-start
			closeDone <- q.Close()
		}()
		close(start)
		addErr := <-addDone
		if err := <-closeDone; err != nil {
			t.Fatal(err)
		}
		want := int32(0)
		if addErr == nil {
			want = 1
		}
		if got := calls.Load(); got != want {
			t.Fatalf("iteration %d: cleanup calls = %d, want %d (add error %v)", i, got, want, addErr)
		}
	}
}

func TestQueryCloseRetriesRealTemporaryStorage(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("uses Windows sharing violations for real temporary-file removal failure")
	}
	m := memory.NewManager(1024)
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 1024, DefaultResourceGroup: 1024, DefaultQuery: 1024})
	storage, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	f, err := tempScope.CreateFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("spill")); err != nil {
		t.Fatal(err)
	}
	blocker, err := os.Open(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	q := NewWithTemp(context.Background(), m, tempScope)
	buffer, err := memory.NewBufferManager(nil).Allocate(q.Memory, memory.TagMetadata, 32)
	if err != nil {
		_ = blocker.Close()
		t.Fatal(err)
	}
	cleanupCalls := 0
	if err := q.AddCleanup(func() error { cleanupCalls++; buffer.Release(); return nil }); err != nil {
		_ = blocker.Close()
		t.Fatal(err)
	}
	defer func() {
		_ = blocker.Close()
		_ = q.Close()
	}()

	if err := q.Close(); err == nil {
		t.Fatal("expected sharing violation from temporary-file removal")
	}
	if cleanupCalls != 1 {
		t.Fatalf("successful callback calls = %d, want 1", cleanupCalls)
	}
	if got := m.Used(); got != 0 {
		t.Fatalf("managed quota after first close = %d, want 0", got)
	}
	if got := quotas.GlobalUsed(); got != 5 {
		t.Fatalf("temporary quota retained for retry = %d, want 5", got)
	}
	if _, err := tempScope.CreateFile(); !errors.Is(err, storagetemp.ErrClosed) {
		t.Fatalf("temporary scope accepted new file during failed cleanup: %v", err)
	}
	if _, err := q.Memory.Reserve(memory.TagVector, 1); !errors.Is(err, memory.ErrClosed) {
		t.Fatalf("successfully closed memory scope accepted reservation: %v", err)
	}

	if err := blocker.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("temporary cleanup retry failed: %v", err)
	}
	if cleanupCalls != 1 {
		t.Fatalf("successful callback repeated during temporary retry: %d", cleanupCalls)
	}
	if got := m.Used(); got != 0 {
		t.Fatalf("managed quota after retry = %d, want 0", got)
	}
	if got := quotas.GlobalUsed(); got != 0 {
		t.Fatalf("temporary quota after retry = %d, want 0", got)
	}
	if got := tempScope.OwnedFiles(); got != 0 {
		t.Fatalf("temporary files after retry = %d, want 0", got)
	}
	if _, err := os.Stat(tempScope.Dir()); !os.IsNotExist(err) {
		t.Fatalf("temporary directory remains after retry: %v", err)
	}
}
