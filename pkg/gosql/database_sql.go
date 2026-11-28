package gosql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/execution/result"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/session"
	"github.com/steamwo/fssql/internal/sql/parser"
)

// OpenSQLDB returns the database/sql compatibility boundary over the same
// native DB execution path. It does not register a process-global driver name.
func OpenSQLDB(config Config) (*sql.DB, error) {
	native, err := Open(config)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(&sqlConnector{db: native}), nil
}

type sqlConnector struct{ db *DB }

// database/sql closes an io.Closer connector when its pool is closed.
func (c *sqlConnector) Close() error { return c.db.Close() }

func (c *sqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if c == nil || c.db == nil || (c.db.closed.Load()||(c.db.owner!=nil&&c.db.owner.closed.Load())) {
		return nil, errors.New("gosql: SQL connector is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return newSQLConnection(c.db)
}
func (c *sqlConnector) Driver() driver.Driver { return sqlDriver{db: c.db} }

type sqlDriver struct{ db *DB }

func (d sqlDriver) Open(string) (driver.Conn, error) {
	if d.db == nil || d.db.closed.Load() {
		return nil, errors.New("gosql: SQL driver is closed")
	}
	return newSQLConnection(d.db)
}

type sqlConn struct {
	db     *DB
	closed bool
	mu     sync.Mutex
}

func (c *sqlConn) Prepare(sqlText string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), sqlText)
}
func (c *sqlConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.db.engine.CloseSession()
}
func (c *sqlConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *sqlConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("gosql: transactions are not supported")
}

func (c *sqlConn) Ping(ctx context.Context) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed || c.db == nil || (c.db.closed.Load()||(c.db.owner!=nil&&c.db.owner.closed.Load())) {
		return errors.New("gosql: SQL connection is closed")
	}
	return ctx.Err()
}

func (c *sqlConn) ExecContext(ctx context.Context, sqlText string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	template, err := parser.Compile(sqlText)
	if err != nil {
		return nil, err
	}
	return c.execTemplate(ctx, sqlText, template, args)
}

func (c *sqlConn) QueryContext(ctx context.Context, sqlText string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	template, err := parser.Compile(sqlText)
	if err != nil {
		return nil, err
	}
	return c.queryTemplate(ctx, sqlText, template, args)
}

func (c *sqlConn) queryTemplate(ctx context.Context, sqlText string, template *parser.Template, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	parsed, err := bindSQLParameters(template, args)
	if err != nil {
		return nil, err
	}
	q, prepared, columns, err := c.db.prepareParsed(ctx, sqlText, parsed)
	if err != nil {
		return nil, err
	}
	typeNames := make([]string, len(columns))
	for i, column := range prepared.Output() {
		typeNames[i], err = result.DuckDBTypeName(column.Type)
		if err != nil {
			return nil, errors.Join(err, q.Close())
		}
	}
	streamCtx, cancel := context.WithCancel(q.Context())
	sink := &sqlChunkSink{ctx: streamCtx, chunks: make(chan *vector.DataChunk, 1)}
	rows := &sqlRows{columns: columns, typeNames: typeNames, q: q, cancel: cancel, sink: sink, done: make(chan struct{})}
	go rows.run(prepared)
	return rows, nil
}

func (c *sqlConn) check() error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed || c.db == nil || (c.db.closed.Load()||(c.db.owner!=nil&&c.db.owner.closed.Load())) {
		return errors.New("gosql: SQL connection is closed")
	}
	return nil
}

var (
	_ driver.Connector          = (*sqlConnector)(nil)
	_ driver.Driver             = sqlDriver{}
	_ driver.Conn               = (*sqlConn)(nil)
	_ driver.ConnPrepareContext = (*sqlConn)(nil)
	_ driver.ConnBeginTx        = (*sqlConn)(nil)
	_ driver.ExecerContext      = (*sqlConn)(nil)
	_ driver.QueryerContext     = (*sqlConn)(nil)
	_ driver.Pinger             = (*sqlConn)(nil)
)

type sqlResult struct{}

func (sqlResult) LastInsertId() (int64, error) {
	return 0, errors.New("gosql: LastInsertId is not applicable")
}
func (sqlResult) RowsAffected() (int64, error) { return 0, nil }

type sqlChunkSink struct {
	ctx    context.Context
	chunks chan *vector.DataChunk
}

func (s *sqlChunkSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	select {
	case s.chunks <- chunk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (*sqlChunkSink) Finalize(context.Context) error { return nil }
func (*sqlChunkSink) TakesChunkOwnership() bool      { return true }

var _ pipeline.OwnershipTakingSink = (*sqlChunkSink)(nil)

type sqlRows struct {
	columns   []Column
	typeNames []string
	q         *query.Context
	cancel    context.CancelFunc
	sink      *sqlChunkSink
	done      chan struct{}

	current     *vector.DataChunk
	row         int
	terminalErr error
	closeOnce   sync.Once
	mu          sync.Mutex
}

func (r *sqlRows) run(prepared session.PreparedStatement) {
	err := prepared.Run(r.q, r.sink)
	r.mu.Lock()
	r.terminalErr = err
	r.mu.Unlock()
	close(r.sink.chunks)
	close(r.done)
}

func (r *sqlRows) Columns() []string {
	out := make([]string, len(r.columns))
	for i := range r.columns {
		out[i] = r.columns[i].Name
	}
	return out
}

func (r *sqlRows) Close() error {
	var closeErr error
	r.closeOnce.Do(func() {
		// Sources and operators run on the query context, including readers that
		// are blocked before producing their next chunk.
		r.q.Cancel()
		r.cancel()
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		for chunk := range r.sink.chunks {
			if chunk != nil {
				chunk.Release()
			}
		}
		<-r.done
		closeErr = r.q.Close()
	})
	return closeErr
}

func (r *sqlRows) Next(dest []driver.Value) error {
	if len(dest) != len(r.columns) {
		return fmt.Errorf("gosql: database/sql destination columns=%d want=%d", len(dest), len(r.columns))
	}
	for {
		if r.current != nil && r.row < r.current.Cardinality {
			for column := range dest {
				value, err := sqlDriverValue(r.current.Columns[column], r.row)
				if err != nil {
					return err
				}
				dest[column] = value
			}
			r.row++
			return nil
		}
		if r.current != nil {
			r.current.Release()
			r.current = nil
			r.row = 0
		}
		chunk, ok := <-r.sink.chunks
		if !ok {
			r.mu.Lock()
			err := r.terminalErr
			r.mu.Unlock()
			closeErr := r.Close()
			if err != nil {
				return errors.Join(err, closeErr)
			}
			if closeErr != nil {
				return closeErr
			}
			return io.EOF
		}
		if chunk == nil {
			continue
		}
		r.current = chunk
	}
}

func (r *sqlRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return r.typeNames[index]
}
func (r *sqlRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return false, false
	}
	return r.columns[index].Nullable, true
}

var (
	_ driver.Rows                           = (*sqlRows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*sqlRows)(nil)
	_ driver.RowsColumnTypeNullable         = (*sqlRows)(nil)
)

func sqlDriverValue(v *vector.Vector, row int) (driver.Value, error) {
	if !v.IsValid(row) {
		return nil, nil
	}
	if v.Type.IsUnsignedInteger() {
		switch v.Type.ID {
		case types.TinyInt:
			x, _ := v.Uint8(row)
			return int64(x), nil
		case types.SmallInt:
			x, _ := v.Uint16(row)
			return int64(x), nil
		case types.Integer:
			x, _ := v.Uint32(row)
			return int64(x), nil
		case types.BigInt:
			x, _ := v.Uint64(row)
			// database/sql has no uint64 driver.Value. Preserve UBIGINT exactly.
			return strconv.FormatUint(x, 10), nil
		}
	}
	if v.Type.IsBlob() {
		x, _ := v.Bytes(row)
		return append([]byte(nil), x...), nil
	}
	switch v.Type.ID {
	case types.Boolean:
		x, _ := v.Bool(row)
		return x, nil
	case types.TinyInt:
		x, _ := v.Int8(row)
		return int64(x), nil
	case types.SmallInt:
		x, _ := v.Int16(row)
		return int64(x), nil
	case types.Integer:
		x, _ := v.Int32(row)
		return int64(x), nil
	case types.BigInt:
		x, _ := v.Int64(row)
		return x, nil
	case types.Float:
		x, _ := v.Float32(row)
		return float64(x), nil
	case types.Double:
		x, _ := v.Float64(row)
		return x, nil
	case types.Varchar:
		x, _ := v.String(row)
		return x, nil
	case types.DateType:
		days, _ := v.Int32(row)
		if sign:=semantics.DateInfinity(int64(days));sign!=0{return semantics.InfinityText(sign),nil}
		return time.Unix(int64(days)*86400, 0).UTC(), nil
	case types.TimestampType:
		raw, _ := v.Int64(row)
		if sign:=semantics.TimestampInfinity(raw);sign!=0{return semantics.InfinityText(sign),nil}
		switch v.Type.Timestamp.Unit {
		case types.TimestampNanos:
			seconds := raw / 1_000_000_000
			nanos := raw % 1_000_000_000
			return time.Unix(seconds, nanos).UTC(), nil
		case types.TimestampMillis:
			return time.UnixMilli(raw).UTC(), nil
		default:
			return time.UnixMicro(raw).UTC(), nil
		}
	case types.Decimal:
		if v.Type.Decimal.Width <= 18 {
			raw, _ := v.Int64(row)
			return formatScaledInteger(strconv.FormatInt(raw, 10), int(v.Type.Decimal.Scale)), nil
		}
		raw, _ := v.Int128(row)
		return formatScaledInteger((Int128{Lo: raw.Lo, Hi: raw.Hi}).String(), int(v.Type.Decimal.Scale)), nil
	case types.HugeInt:
		raw, _ := v.Int128(row)
		return (Int128{Lo: raw.Lo, Hi: raw.Hi}).String(), nil
	case types.List, types.Struct:
		return nil, fmt.Errorf("gosql: database/sql does not expose %s values; use QueryChunks", logicalTypeNameOf(v.Type))
	default:
		return nil, fmt.Errorf("gosql: unsupported database/sql type %s", logicalTypeNameOf(v.Type))
	}
}

func formatScaledInteger(raw string, scale int) string {
	if scale <= 0 {
		return raw
	}
	negative := strings.HasPrefix(raw, "-")
	digits := strings.TrimPrefix(raw, "-")
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	split := len(digits) - scale
	out := digits[:split] + "." + digits[split:]
	if negative && out != "0" && out != "0."+strings.Repeat("0", scale) {
		out = "-" + out
	}
	return out
}

var _ = math.MaxFloat64
