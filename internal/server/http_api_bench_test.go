package server

import "testing"

// BenchmarkParseQueryPath 记录 HTTP Query API 的无分配路由解析基线。
func BenchmarkParseQueryPath(b *testing.B) {
	const path = "/v1/queries/0123456789abcdef0123456789abcdef/results"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id, suffix, ok := parseQueryPath(path)
		if !ok || id == "" || suffix != "results" {
			b.Fatal("unexpected route parse result")
		}
	}
}
