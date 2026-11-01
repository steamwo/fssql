package sftp

import (
	"context"
	"testing"
)

func BenchmarkSessionPoolAcquireRelease(b *testing.B) {
	dial := &fakeDialer{key: "SHA256:good", data: []byte("abc")}
	p := newSessionPool(8, dial, DialConfig{Endpoint: "sftp://host", CredentialScope: "x", SecurityProfile: "x"})
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l, _, err := p.acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}
		l.Release()
	}
	p.close()
}
