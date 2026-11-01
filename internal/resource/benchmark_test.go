package resource

import (
	"context"
	"testing"
)

// BenchmarkAcquireRelease 覆盖无排队情况下 Global Resource Scheduler admission/release 控制路径。
func BenchmarkAcquireRelease(b *testing.B) {
	budget := Budget{
		ConcurrentQueries: Unlimited,
		CPUSlots:          Unlimited,
		MemoryBytes:       Unlimited,
		RemoteIOSlots:     Unlimited,
		SpillIOSlots:      Unlimited,
	}
	scheduler, err := NewScheduler(budget, []ResourceGroup{{Name: "default", Weight: 1, Budget: budget}})
	if err != nil {
		b.Fatal(err)
	}
	request := Request{
		QueryID: "bench", Group: "default", Class: QueryClassInteractive,
		CPUSlots: 1, MemoryBytes: 1024, RemoteIOSlots: 1, SpillIOSlots: 1,
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, err := scheduler.Acquire(ctx, request)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}
