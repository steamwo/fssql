package gosql

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// hmacSHA256Reference 使用旧标量算法生成兼容对照值。
func hmacSHA256Reference(key []byte, value string) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

// TestHMACSHA256DirectEvalCompatibility 验证公共直接 Eval 的 NULL、字节和 key 所有权语义。
func TestHMACSHA256DirectEvalCompatibility(t *testing.T) {
	key := []byte("direct-eval-key")
	definition, err := NewHMACSHA256ScalarFunction(key)
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 'X'
	ownedKey := []byte("direct-eval-key")
	for _, tc := range []struct {
		name  string
		value string
		valid bool
	}{
		{name: "null", valid: false},
		{name: "empty", value: "", valid: true},
		{name: "unicode", value: "你好😊-e\u0301", valid: true},
		{name: "invalid_utf8", value: string([]byte{0xff, 0x00, 0xfe, 'x'}), valid: true},
		{name: "known", value: "what do ya want for nothing?", valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := definition.Eval([]ScalarValue{{Type: Type{ID: "VARCHAR"}, Valid: tc.valid, String: tc.value}})
			if err != nil {
				t.Fatal(err)
			}
			if got.Type.ID != "VARCHAR" || got.Valid != tc.valid {
				t.Fatalf("result type=%q valid=%v, want VARCHAR/%v", got.Type.ID, got.Valid, tc.valid)
			}
			if tc.valid {
				if want := hmacSHA256Reference(ownedKey, tc.value); got.String != want {
					t.Fatalf("HMAC=%q want=%q", got.String, want)
				}
			}
		})
	}
	twoValues := []ScalarValue{{Type: Type{ID: "VARCHAR"}, Valid: true}, {Type: Type{ID: "VARCHAR"}, Valid: true}}
	for _, values := range [][]ScalarValue{nil, twoValues} {
		got, err := definition.Eval(values)
		if err != nil || got.Valid {
			t.Fatalf("invalid arity result=%+v err=%v", got, err)
		}
	}
}

// TestHMACSHA256DirectEvalConcurrentIsolation 验证并发直接 Eval 不共享计算状态或混用 key。
func TestHMACSHA256DirectEvalConcurrentIsolation(t *testing.T) {
	keys := []string{"direct-concurrent-key-a", "direct-concurrent-key-b"}
	definitions := make([]ScalarFunction, len(keys))
	for i, key := range keys {
		definition, err := NewHMACSHA256ScalarFunction([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		definitions[i] = definition
	}

	const goroutines = 16
	const iterations = 64
	start := make(chan struct{})
	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	for worker := 0; worker < goroutines; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				definitionIndex := (worker + iteration) % len(definitions)
				value := fmt.Sprintf("worker=%d/iteration=%d/你好😊", worker, iteration)
				got, err := definitions[definitionIndex].Eval([]ScalarValue{{Type: Type{ID: "VARCHAR"}, Valid: true, String: value}})
				if err != nil {
					errCh <- fmt.Errorf("worker=%d iteration=%d Eval: %w", worker, iteration, err)
					return
				}
				want := hmacSHA256Reference([]byte(keys[definitionIndex]), value)
				if !got.Valid || got.String != want {
					errCh <- fmt.Errorf("worker=%d iteration=%d key=%d HMAC=%q valid=%v want=%q", worker, iteration, definitionIndex, got.String, got.Valid, want)
					return
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestHMACSHA256FactoryEvalReplacement 验证 SQL 始终调用工厂实例的当前 Eval，并保留原工厂算法。
func TestHMACSHA256FactoryEvalReplacement(t *testing.T) {
	t.Run("original_factory", func(t *testing.T) {
		definition, err := NewHMACSHA256ScalarFunction([]byte("original-factory-key"))
		if err != nil {
			t.Fatal(err)
		}
		const message = "original-你好😊"
		want := hmacSHA256Reference([]byte("original-factory-key"), message)
		direct, err := definition.Eval([]ScalarValue{{Type: Type{ID: "VARCHAR"}, Valid: true, String: message}})
		if err != nil || !direct.Valid || direct.String != want {
			t.Fatalf("direct result=%+v err=%v want=%q", direct, err, want)
		}
		db, err := Open(Config{ScalarFunctions: []ScalarFunction{definition}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		_, err = db.QueryChunks(context.Background(), "SELECT data_plane_hmac_sha256('original-你好😊')", func(chunk *Chunk) error {
			got, valid, err := chunk.String(0, 0)
			if err != nil || !valid || got != want {
				return fmt.Errorf("SQL HMAC=%q valid=%v err=%v want=%q", got, valid, err, want)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("managed memory leak: %d", used)
		}
	})

	t.Run("custom_success", func(t *testing.T) {
		definition, err := NewHMACSHA256ScalarFunction([]byte("replace-success-key"))
		if err != nil {
			t.Fatal(err)
		}
		var calls atomic.Int32
		definition.Eval = func(values []ScalarValue) (ScalarValue, error) {
			calls.Add(1)
			if len(values) != 1 || !values[0].Valid {
				return ScalarValue{}, errors.New("replacement received invalid arguments")
			}
			return ScalarValue{Type: Type{ID: "VARCHAR"}, Valid: true, String: "replacement:" + values[0].String}, nil
		}
		input := []ScalarValue{{Type: Type{ID: "VARCHAR"}, Valid: true, String: "custom-你好"}}
		want, err := definition.Eval(input)
		if err != nil || !want.Valid {
			t.Fatalf("direct replacement result=%+v err=%v", want, err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("direct replacement calls=%d want=1", got)
		}
		db, err := Open(Config{ScalarFunctions: []ScalarFunction{definition}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		seen := 0
		_, err = db.QueryChunks(context.Background(), "SELECT data_plane_hmac_sha256('custom-你好')", func(chunk *Chunk) error {
			seen++
			got, valid, err := chunk.String(0, 0)
			if err != nil || !valid || got != want.String {
				return fmt.Errorf("SQL replacement=%q valid=%v err=%v want=%q", got, valid, err, want.String)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if seen != 1 {
			t.Fatalf("result chunks=%d want=1", seen)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("replacement calls=%d want=2 (direct and SQL)", got)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("managed memory leak: %d", used)
		}
	})

	t.Run("sentinel_error", func(t *testing.T) {
		definition, err := NewHMACSHA256ScalarFunction([]byte("replace-error-key"))
		if err != nil {
			t.Fatal(err)
		}
		sentinel := errors.New("replacement sentinel")
		var calls atomic.Int32
		definition.Eval = func([]ScalarValue) (ScalarValue, error) {
			calls.Add(1)
			return ScalarValue{}, sentinel
		}
		_, directErr := definition.Eval([]ScalarValue{{Type: Type{ID: "VARCHAR"}, Valid: true, String: "boom"}})
		if directErr != sentinel {
			t.Fatalf("direct error=%v want sentinel=%v", directErr, sentinel)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("direct replacement calls=%d want=1", got)
		}
		db, err := Open(Config{ScalarFunctions: []ScalarFunction{definition}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		callbackCalled := false
		_, sqlErr := db.QueryChunks(context.Background(), "SELECT data_plane_hmac_sha256('boom')", func(*Chunk) error {
			callbackCalled = true
			return nil
		})
		if !errors.Is(sqlErr, sentinel) {
			t.Fatalf("SQL error=%v want sentinel=%v", sqlErr, sentinel)
		}
		if callbackCalled {
			t.Fatal("SQL emitted a result after replacement returned sentinel")
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("replacement calls=%d want=2 (direct and SQL)", got)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("managed memory leak after replacement error: %d", used)
		}
	})
}

// TestHMACSHA256RegisteredBridgeMatchesDirectEval 验证通用注册桥接逐行调用当前 Eval，且输出不借用输入。
func TestHMACSHA256RegisteredBridgeMatchesDirectEval(t *testing.T) {
	key := []byte("batch-key")
	definition, err := NewHMACSHA256ScalarFunction(key)
	if err != nil {
		t.Fatal(err)
	}
	_, registry, err := registerScalarFunctions(nil, []ScalarFunction{definition})
	if err != nil {
		t.Fatal(err)
	}
	values := []struct {
		text  string
		valid bool
	}{
		{text: "ignored", valid: false},
		{text: "", valid: true},
		{text: "你好😊", valid: true},
		{text: string([]byte{0xff, 0x00, 0xfe, 'x'}), valid: true},
		{text: "a value longer than inline storage", valid: true},
	}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	text := types.LogicalType{ID: types.Varchar}
	input, err := vector.NewFlat(scope, buffers, text, len(values))
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]string, len(values))
	for row, value := range values {
		if !value.valid {
			input.SetNull(row)
			continue
		}
		if err := input.SetString(row, value.text); err != nil {
			t.Fatal(err)
		}
		direct, err := definition.Eval([]ScalarValue{{Type: Type{ID: "VARCHAR"}, Valid: true, String: value.text}})
		if err != nil || !direct.Valid {
			t.Fatalf("direct row %d result=%+v err=%v", row, direct, err)
		}
		expected[row] = direct.String
	}
	chunk, err := vector.NewDataChunk(input)
	if err != nil {
		t.Fatal(err)
	}
	expr := &binder.BoundExpression{
		Kind:         binder.BoundFunction,
		Type:         text,
		FunctionID:   function.FirstDynamicScalarID,
		FunctionKind: function.Scalar,
		Children:     []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: text, ColumnIndex: 0}},
	}
	executor := expression.NewExecutor(scope, buffers)
	executor.Scalars = registry
	out, err := executor.ExecuteSQL(expr, chunk)
	if err != nil {
		chunk.Release()
		t.Fatal(err)
	}
	chunk.Release()
	for row, value := range values {
		valid := out.IsValid(row)
		if valid != value.valid {
			out.Release()
			t.Fatalf("row %d valid=%v want=%v", row, valid, value.valid)
		}
		if !valid {
			continue
		}
		encoded, _ := out.Bytes(row)
		got := string(encoded)
		if got != expected[row] {
			out.Release()
			t.Fatalf("row %d HMAC=%q want=%q", row, got, expected[row])
		}
	}
	out.Release()
	if used := scope.Used(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
}

// TestHMACSHA256NameDoesNotSpecializeCustomUDF 验证同名自定义 UDF 保留 NULLCall 和逐行 Eval。
func TestHMACSHA256NameDoesNotSpecializeCustomUDF(t *testing.T) {
	var calls atomic.Int32
	custom := ScalarFunction{
		Name:         "data_plane_hmac_sha256",
		Arguments:    []Type{{ID: "VARCHAR"}},
		Result:       Type{ID: "VARCHAR"},
		NullHandling: ScalarNullCall,
		Eval: func(values []ScalarValue) (ScalarValue, error) {
			calls.Add(1)
			if len(values) != 1 {
				return ScalarValue{}, errors.New("custom UDF arity mismatch")
			}
			if !values[0].Valid {
				return ScalarValue{Type: Type{ID: "VARCHAR"}, Valid: true, String: "custom-null"}, nil
			}
			return ScalarValue{Type: Type{ID: "VARCHAR"}, Valid: true, String: "custom:" + values[0].String}, nil
		},
	}
	db, err := Open(Config{ScalarFunctions: []ScalarFunction{custom}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.QueryChunks(context.Background(), "SELECT data_plane_hmac_sha256('x'), data_plane_hmac_sha256(CAST(NULL AS VARCHAR))", func(chunk *Chunk) error {
		for column, want := range []string{"custom:x", "custom-null"} {
			got, valid, err := chunk.String(0, column)
			if err != nil || !valid || got != want {
				return fmt.Errorf("column %d=%q valid=%v err=%v want=%q", column, got, valid, err, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("custom Eval calls=%d want=2", got)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
}

// TestHMACSHA256SameDBMultipleKeysAndConcurrentQueries 验证注册 ID 与 key 绑定且并发状态隔离。
func TestHMACSHA256SameDBMultipleKeysAndConcurrentQueries(t *testing.T) {
	first, err := NewHMACSHA256ScalarFunction([]byte("first-key"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHMACSHA256ScalarFunction([]byte("second-key"))
	if err != nil {
		t.Fatal(err)
	}
	first.Name = "hmac_first"
	second.Name = "hmac_second"
	db, err := Open(Config{ScalarFunctions: []ScalarFunction{first, second}, Workers: 4, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const message = "你好😊"
	expected := []string{
		hmacSHA256Reference([]byte("first-key"), message),
		hmacSHA256Reference([]byte("second-key"), message),
		hmacSHA256Reference([]byte("first-key"), ""),
	}
	const goroutines = 8
	const iterations = 12
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for worker := 0; worker < goroutines; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				_, err := db.QueryChunks(context.Background(), "SELECT hmac_first('你好😊'), hmac_second('你好😊'), hmac_first('')", func(chunk *Chunk) error {
					for column, want := range expected {
						got, valid, err := chunk.String(0, column)
						if err != nil || !valid || got != want {
							return fmt.Errorf("column %d=%q valid=%v err=%v want=%q", column, got, valid, err, want)
						}
					}
					return nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
}

// TestHMACSHA256IndependentDBIsolation 验证独立 DB 的同名 HMAC 不混用 key。
func TestHMACSHA256IndependentDBIsolation(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  string
	}{{"db_a", "database-a-key"}, {"db_b", "database-b-key"}} {
		t.Run(tc.name, func(t *testing.T) {
			definition, err := NewHMACSHA256ScalarFunction([]byte(tc.key))
			if err != nil {
				t.Fatal(err)
			}
			db, err := Open(Config{ScalarFunctions: []ScalarFunction{definition}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			_, err = db.QueryChunks(context.Background(), "SELECT data_plane_hmac_sha256('shared-message')", func(chunk *Chunk) error {
				got, valid, err := chunk.String(0, 0)
				if err != nil || !valid {
					return fmt.Errorf("HMAC valid=%v err=%v", valid, err)
				}
				if want := hmacSHA256Reference([]byte(tc.key), "shared-message"); got != want {
					return fmt.Errorf("HMAC=%q want=%q", got, want)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if used := db.MemoryUsed(); used != 0 {
				t.Fatalf("managed memory leak: %d", used)
			}
		})
	}
}

// TestHMACSHA256RegisteredBridgeReleasesOutputAfterLimit 验证通用逐行输出遇到限额失败会完整释放。
func TestHMACSHA256RegisteredBridgeReleasesOutputAfterLimit(t *testing.T) {
	definition, err := NewHMACSHA256ScalarFunction([]byte("limit-key"))
	if err != nil {
		t.Fatal(err)
	}
	_, registry, err := registerScalarFunctions(nil, []ScalarFunction{definition})
	if err != nil {
		t.Fatal(err)
	}
	const rows = 8
	// 输入占用 704 字节；1024 允许创建输出向量，但会在逐行增长字符串堆时触发限额。
	manager := memory.NewManager(1024)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	text := types.LogicalType{ID: types.Varchar}
	input, err := vector.NewFlat(scope, buffers, text, rows)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < rows; row++ {
		if err := input.SetString(row, "ABCDEFGHIJKLMNOPQRSTUVWXYZ-abcdefghijklmnopqrstuvwxyz"); err != nil {
			t.Fatal(err)
		}
	}
	chunk, err := vector.NewDataChunk(input)
	if err != nil {
		t.Fatal(err)
	}
	inputManaged := manager.Used()
	if inputManaged != 704 {
		chunk.Release()
		t.Fatalf("unexpected input reservation: %d", inputManaged)
	}
	expr := &binder.BoundExpression{
		Kind:         binder.BoundFunction,
		Type:         text,
		FunctionID:   function.FirstDynamicScalarID,
		FunctionKind: function.Scalar,
		Children:     []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: text, ColumnIndex: 0}},
	}
	executor := expression.NewExecutor(scope, buffers)
	executor.Scalars = registry
	out, err := executor.ExecuteSQL(expr, chunk)
	if out != nil {
		out.Release()
		chunk.Release()
		t.Fatal("limit error returned an output vector")
	}
	if !errors.Is(err, memory.ErrLimitExceeded) {
		chunk.Release()
		t.Fatalf("error=%v want=%v", err, memory.ErrLimitExceeded)
	}
	if used := manager.Used(); used != inputManaged {
		chunk.Release()
		t.Fatalf("output was not released: managed=%d input=%d", used, inputManaged)
	}
	chunk.Release()
	if used := manager.Used(); used != 0 {
		t.Fatalf("managed memory after cleanup=%d", used)
	}
}

// TestHMACSHA256RegisteredBridgeVectorLayouts 验证通用桥接处理常量与字典输入时逐行结果一致。
func TestHMACSHA256RegisteredBridgeVectorLayouts(t *testing.T) {
	key := []byte("layout-key")
	definition, err := NewHMACSHA256ScalarFunction(key)
	if err != nil {
		t.Fatal(err)
	}
	_, registry, err := registerScalarFunctions(nil, []ScalarFunction{definition})
	if err != nil {
		t.Fatal(err)
	}
	text := types.LogicalType{ID: types.Varchar}
	expr := &binder.BoundExpression{
		Kind:         binder.BoundFunction,
		Type:         text,
		FunctionID:   function.FirstDynamicScalarID,
		FunctionKind: function.Scalar,
		Children:     []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: text, ColumnIndex: 0}},
	}
	for _, layout := range []string{"constant", "dictionary"} {
		t.Run(layout, func(t *testing.T) {
			manager := memory.NewManager(1 << 20)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			var input, base *vector.Vector
			var expected []struct {
				text  string
				valid bool
			}
			if layout == "constant" {
				input, err = vector.NewConstant(scope, buffers, text, 4)
				if err == nil {
					err = input.SetString(0, "fixed")
				}
				for range 4 {
					expected = append(expected, struct {
						text  string
						valid bool
					}{text: "fixed", valid: true})
				}
			} else {
				base, err = vector.NewFlat(scope, buffers, text, 3)
				if err != nil {
					t.Fatal(err)
				}
				for row, value := range []string{"first", "", "ignored"} {
					if err := base.SetString(row, value); err != nil {
						t.Fatal(err)
					}
				}
				base.SetNull(2)
				input, err = vector.NewDictionary(base, vector.SelectionFrom([]uint32{2, 1, 0, 1}))
				expected = []struct {
					text  string
					valid bool
				}{{}, {text: "", valid: true}, {text: "first", valid: true}, {text: "", valid: true}}
			}
			if err != nil {
				t.Fatal(err)
			}
			chunk, err := vector.NewDataChunk(input)
			if err != nil {
				t.Fatal(err)
			}
			executor := expression.NewExecutor(scope, buffers)
			executor.Scalars = registry
			out, err := executor.ExecuteSQL(expr, chunk)
			if err != nil {
				t.Fatal(err)
			}
			for row, want := range expected {
				if out.IsValid(row) != want.valid {
					t.Fatalf("row=%d validity=%v want=%v", row, out.IsValid(row), want.valid)
				}
				if !want.valid {
					continue
				}
				encoded, _ := out.Bytes(row)
				if wantHMAC := hmacSHA256Reference(key, want.text); string(encoded) != wantHMAC {
					t.Fatalf("row=%d HMAC=%q want=%q", row, encoded, wantHMAC)
				}
			}
			out.Release()
			chunk.Release()
			if base != nil {
				base.Release()
			}
			if got := manager.Used(); got != 0 {
				t.Fatalf("managed leak=%d", got)
			}
		})
	}
}

// BenchmarkHMACSHA256Batch2048 测量经通用逐行注册桥接和 ExecuteSQL 的 2048 行 HMAC 开销。
func BenchmarkHMACSHA256Batch2048(b *testing.B) {
	definition, err := NewHMACSHA256ScalarFunction([]byte("benchmark-hmac-key"))
	if err != nil {
		b.Fatal(err)
	}
	_, registry, err := registerScalarFunctions(nil, []ScalarFunction{definition})
	if err != nil {
		b.Fatal(err)
	}
	manager := memory.NewManager(8 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	text := types.LogicalType{ID: types.Varchar}
	input, err := vector.NewFlat(scope, buffers, text, 2048)
	if err != nil {
		b.Fatal(err)
	}
	for row := 0; row < 2048; row++ {
		if err := input.SetString(row, "prefix-β-middle-😊-a string longer than inline storage"); err != nil {
			b.Fatal(err)
		}
	}
	chunk, err := vector.NewDataChunk(input)
	if err != nil {
		b.Fatal(err)
	}
	expr := &binder.BoundExpression{
		Kind:         binder.BoundFunction,
		Type:         text,
		FunctionID:   function.FirstDynamicScalarID,
		FunctionKind: function.Scalar,
		Children: []*binder.BoundExpression{
			{Kind: binder.BoundColumn, Type: text, ColumnIndex: 0},
		},
	}
	executor := expression.NewExecutor(scope, buffers)
	executor.Scalars = registry
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		out, err := executor.ExecuteSQL(expr, chunk)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
	b.StopTimer()
	b.ReportMetric(float64(manager.Peak()), "managed-peak-B")
	chunk.Release()
	if used := scope.Used(); used != 0 {
		b.Fatalf("managed memory leak: %d", used)
	}
}
