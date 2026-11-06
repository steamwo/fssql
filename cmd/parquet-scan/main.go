package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/storage/fileset"
	remotes3 "github.com/steamwo/fssql/internal/storage/remote/s3"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type config struct {
	mode       string
	source     string
	endpoint   string
	region     string
	columns    string
	union      bool
	hive       bool
	fullSchema bool
	pathStyle  bool
	allowHTTP  bool
	limit      int64
	batch      int
	memory     int64
	accessKey  string
	secretKey  string
	session    string
}

func main() {
	cfg := parseFlags()
	if err := run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", "scan", "describe|explain|analyze|scan")
	flag.StringVar(&cfg.source, "source", "", "S3 Parquet URI or glob, e.g. s3://bucket/path/*.parquet")
	flag.StringVar(&cfg.endpoint, "endpoint", "https://s3.amazonaws.com", "S3 endpoint")
	flag.StringVar(&cfg.region, "region", getenv("AWS_REGION", "us-east-1"), "S3 region")
	flag.StringVar(&cfg.columns, "columns", "", "comma-separated top-level or dotted columns")
	flag.BoolVar(&cfg.union, "union-by-name", false, "merge compatible schemas by name")
	flag.BoolVar(&cfg.hive, "hive", true, "discover Hive key=value partitions")
	flag.BoolVar(&cfg.fullSchema, "full-schema-validation", false, "validate every matched file schema")
	flag.BoolVar(&cfg.pathStyle, "path-style", false, "use path-style S3 addressing")
	flag.BoolVar(&cfg.allowHTTP, "allow-http", false, "allow explicit insecure HTTP endpoint")
	flag.Int64Var(&cfg.limit, "limit", 100, "maximum rows to print in scan mode; 0 means no limit")
	flag.IntVar(&cfg.batch, "batch-size", vector.DefaultVectorSize, "Parquet output batch size")
	flag.Int64Var(&cfg.memory, "memory-limit", 256<<20, "managed memory limit in bytes")
	flag.StringVar(&cfg.accessKey, "access-key", os.Getenv("AWS_ACCESS_KEY_ID"), "S3 access key (defaults AWS_ACCESS_KEY_ID)")
	flag.StringVar(&cfg.secretKey, "secret-key", os.Getenv("AWS_SECRET_ACCESS_KEY"), "S3 secret key (defaults AWS_SECRET_ACCESS_KEY)")
	flag.StringVar(&cfg.session, "session-token", os.Getenv("AWS_SESSION_TOKEN"), "S3 session token (defaults AWS_SESSION_TOKEN)")
	flag.Parse()
	return cfg
}

func getenv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func run(ctx context.Context, cfg config) error {
	if cfg.source == "" {
		return errors.New("parquet-scan: -source is required")
	}
	if cfg.batch <= 0 || cfg.batch > vector.DefaultVectorSize {
		return fmt.Errorf("parquet-scan: invalid batch size %d", cfg.batch)
	}
	if cfg.memory <= 0 {
		return errors.New("parquet-scan: memory limit must be positive")
	}

	driver, err := remotes3.NewDriver(remotes3.Config{
		Endpoint:          cfg.endpoint,
		Region:            cfg.region,
		ForcePathStyle:    cfg.pathStyle,
		AllowInsecureHTTP: cfg.allowHTTP,
		CredentialScope:   "parquet-scan",
		CredentialVersion: "env-v1",
		SecurityProfile:   "parquet-scan",
	}, remotes3.Credentials{AccessKey: cfg.accessKey, SecretKey: cfg.secretKey, SessionToken: cfg.session}, nil)
	if err != nil {
		return err
	}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		return err
	}
	cache, err := parquet.NewMetadataCache(256)
	if err != nil {
		return err
	}
	manager := memory.NewManager(cfg.memory)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, err := parquet.NewSourceBinder(registry, cache, scope, bm)
	if err != nil {
		return err
	}

	validation := parquet.SchemaValidationBounded
	if cfg.fullSchema || cfg.mode == "scan" || cfg.mode == "explain" || cfg.mode == "analyze" {
		validation = parquet.SchemaValidationFull
	}
	opts := parquet.SourceBindOptions{
		FileSet: fileset.PlanOptions{HivePartitioning: cfg.hive},
		UnionByName: cfg.union, ValidationMode: validation,
		MaxFooterBytes: parquet.DefaultMaxFooterBytes,
	}

	switch cfg.mode {
	case "describe":
		err = describe(ctx, binder, cfg.source, opts)
	case "explain":
		err = explain(ctx, binder, cfg.source, opts, splitColumns(cfg.columns))
	case "analyze":
		err = analyze(ctx, registry, binder, scope, bm, cfg, opts)
	case "scan":
		err = scanSource(ctx, registry, binder, scope, bm, cfg, opts)
	default:
		err = fmt.Errorf("parquet-scan: unknown mode %q", cfg.mode)
	}
	if err != nil {
		return err
	}
	if used := scope.Used(); used != 0 {
		return fmt.Errorf("parquet-scan: managed memory not released at command end: %d bytes", used)
	}
	return nil
}

func describe(ctx context.Context, binder *parquet.SourceBinder, source string, opts parquet.SourceBindOptions) error {
	columns, report, err := binder.DescribeSource(ctx, source, opts)
	if err != nil {
		return err
	}
	fmt.Printf("matched_files=%d schema_files=%d fingerprint=%s schema_source=%s confidence=%s union_by_name=%v full_validation=%v\n",
		report.MatchedFiles, report.SchemaFiles, report.DatasetFingerprint, report.SchemaSource, report.Confidence, report.UnionByName, report.FullValidation)
	for _, column := range columns {
		fmt.Printf("%s\t%s\n", column.Name, column.TypeName)
	}
	return nil
}

func explain(ctx context.Context, binder *parquet.SourceBinder, source string, opts parquet.SourceBindOptions, columns []string) error {
	binding, err := binder.Bind(ctx, source, opts)
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		for _, field := range binding.Schema().Fields {
			columns = append(columns, field.Name)
		}
	}
	var rowGroups, requests, planned, transferred, useful int64
	for _, metadata := range binding.Metadata() {
		rowGroups += int64(len(metadata.Footer.Metadata.RowGroups))
		requests += metadata.Footer.Metrics.Requests
		planned += metadata.Footer.Metrics.PlannedBytes
		transferred += metadata.Footer.Metrics.TransferredBytes
		useful += metadata.Footer.Metrics.UsefulBytes
	}
	fmt.Printf("matched_files=%d\n", binding.MatchedFiles())
	fmt.Printf("dataset_fingerprint=%s\n", binding.Fingerprint())
	fmt.Printf("projected_columns=%s\n", strings.Join(columns, ","))
	fmt.Printf("rowgroups_total=%d\n", rowGroups)
	fmt.Printf("footer_requests=%d planned_bytes=%d transferred_bytes=%d useful_bytes=%d\n", requests, planned, transferred, useful)
	return nil
}

func analyze(ctx context.Context, registry *vfs.Registry, binder *parquet.SourceBinder, scope *memory.Scope, bm *memory.BufferManager, cfg config, opts parquet.SourceBindOptions) error {
	binding, err := binder.Bind(ctx, cfg.source, opts)
	if err != nil {
		return err
	}
	columns := splitColumns(cfg.columns)
	if len(columns) == 0 {
		for _, field := range binding.Schema().Fields {
			columns = append(columns, field.Name)
		}
	}
	var rowGroupsTotal int64
	footerMetrics := make([]parquet.FooterMetrics, 0, len(binding.Metadata()))
	rowGroupMetrics := make([]parquet.RowGroupScanMetrics, 0)
	for _, metadata := range binding.Metadata() {
		footerMetrics = append(footerMetrics, metadata.Footer.Metrics)
		rowGroupsTotal += int64(len(metadata.Footer.Metadata.RowGroups))
		driver, err := registry.DriverFor(metadata.File.URI)
		if err != nil {
			return err
		}
		for _, rowGroup := range metadata.Footer.Metadata.RowGroups {
			scanner, err := parquet.NewRowGroupScanner(driver, metadata.File.URI, metadata.File.Identity, metadata.Footer.Schema, rowGroup, columns, nil, scope, bm, cfg.batch)
			if err != nil {
				return fmt.Errorf("analyze %s: %w", metadata.File.URI.String(), err)
			}
			for {
				chunk, err := scanner.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					scanner.Close()
					return err
				}
				chunk.Release()
			}
			rowGroupMetrics = append(rowGroupMetrics, scanner.Metrics())
			scanner.Close()
		}
	}
	profile, err := parquet.BuildScanProfile(parquet.ScanProfileInput{
		Binding: binding, ProjectedColumns: columns, RowGroupsTotal: rowGroupsTotal,
		FooterMetrics: footerMetrics, RowGroupMetrics: rowGroupMetrics,
	})
	if err != nil {
		return err
	}
	fmt.Println(profile.Explain(true))
	return nil
}

func scanSource(ctx context.Context, registry *vfs.Registry, binder *parquet.SourceBinder, scope *memory.Scope, bm *memory.BufferManager, cfg config, opts parquet.SourceBindOptions) error {
	binding, err := binder.Bind(ctx, cfg.source, opts)
	if err != nil {
		return err
	}
	columns := splitColumns(cfg.columns)
	if len(columns) == 0 {
		for _, field := range binding.Schema().Fields {
			columns = append(columns, field.Name)
		}
	}

	var emitted int64
	var aggregate parquet.RowGroupScanMetrics
	for _, metadata := range binding.Metadata() {
		driver, err := registry.DriverFor(metadata.File.URI)
		if err != nil {
			return err
		}
		for _, rowGroup := range metadata.Footer.Metadata.RowGroups {
			scanner, err := parquet.NewRowGroupScanner(driver, metadata.File.URI, metadata.File.Identity, metadata.Footer.Schema, rowGroup, columns, nil, scope, bm, cfg.batch)
			if err != nil {
				return fmt.Errorf("scan %s: %w", metadata.File.URI.String(), err)
			}
			for {
				chunk, err := scanner.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					scanner.Close()
					return err
				}
				for row := 0; row < chunk.Cardinality; row++ {
					values := make([]string, len(chunk.Columns))
					for i, column := range chunk.Columns {
						values[i] = formatValue(column, row)
					}
					fmt.Println(strings.Join(values, "\t"))
					emitted++
					if cfg.limit > 0 && emitted >= cfg.limit {
						chunk.Release()
						metrics := scanner.Metrics()
						addMetrics(&aggregate, metrics)
						scanner.Close()
						fmt.Fprintf(os.Stderr, "rows=%d requests=%d planned_bytes=%d transferred_bytes=%d useful_bytes=%d pages=%d\n", emitted, aggregate.Requests, aggregate.PlannedBytes, aggregate.TransferredBytes, aggregate.UsefulBytes, aggregate.PagesRead)
						return nil
					}
				}
				chunk.Release()
			}
			metrics := scanner.Metrics()
			addMetrics(&aggregate, metrics)
			scanner.Close()
		}
	}
	fmt.Fprintf(os.Stderr, "rows=%d requests=%d planned_bytes=%d transferred_bytes=%d useful_bytes=%d pages=%d\n", emitted, aggregate.Requests, aggregate.PlannedBytes, aggregate.TransferredBytes, aggregate.UsefulBytes, aggregate.PagesRead)
	return nil
}

func addMetrics(dst *parquet.RowGroupScanMetrics, src parquet.RowGroupScanMetrics) {
	dst.RowGroupsPruned += src.RowGroupsPruned
	dst.RowsProduced += src.RowsProduced
	dst.Columns += src.Columns
	dst.Requests += src.Requests
	dst.PlannedBytes += src.PlannedBytes
	dst.TransferredBytes += src.TransferredBytes
	dst.UsefulBytes += src.UsefulBytes
	dst.PagesRead += src.PagesRead
	dst.DataValues += src.DataValues
}

func splitColumns(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if name := strings.TrimSpace(part); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func formatValue(v *vector.Vector, row int) string {
	if v == nil || !v.IsValid(row) {
		return "NULL"
	}
	switch v.Type.ID {
	case coretypes.Boolean:
		x, _ := v.Bool(row)
		return strconv.FormatBool(x)
	case coretypes.Integer, coretypes.DateType:
		x, _ := v.Int32(row)
		return strconv.FormatInt(int64(x), 10)
	case coretypes.BigInt, coretypes.TimestampType:
		x, _ := v.Int64(row)
		return strconv.FormatInt(x, 10)
	case coretypes.Decimal:
		x, _ := v.Int64(row)
		return formatDecimal64(x, int(v.Type.Decimal.Scale))
	case coretypes.Float:
		x, _ := v.Float32(row)
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case coretypes.Double:
		x, _ := v.Float64(row)
		return strconv.FormatFloat(x, 'g', -1, 64)
	case coretypes.Varchar:
		x, _ := v.String(row)
		return strconv.Quote(x)
	case coretypes.List:
		entry, ok := v.ListEntry(row)
		children := v.Children()
		if !ok || len(children) != 1 {
			return "[]"
		}
		values := make([]string, 0, entry.Length)
		for i := uint32(0); i < entry.Length; i++ {
			values = append(values, formatValue(children[0], int(entry.Offset+i)))
		}
		return "[" + strings.Join(values, ",") + "]"
	case coretypes.Struct:
		children := v.Children()
		fields := v.Type.Fields
		values := make([]string, 0, len(children))
		for i, child := range children {
			name := fmt.Sprintf("f%d", i)
			if i < len(fields) {
				name = fields[i].Name
			}
			values = append(values, name+":"+formatValue(child, row))
		}
		return "{" + strings.Join(values, ",") + "}"
	default:
		return fmt.Sprintf("<unsupported-type:%d>", v.Type.ID)
	}
}

func formatDecimal64(value int64, scale int) string {
	if scale <= 0 {
		return strconv.FormatInt(value, 10)
	}
	negative := value < 0
	var magnitude uint64
	if negative {
		magnitude = uint64(-(value + 1)) + 1
	} else {
		magnitude = uint64(value)
	}
	digits := strconv.FormatUint(magnitude, 10)
	for len(digits) <= scale {
		digits = "0" + digits
	}
	cut := len(digits) - scale
	out := digits[:cut] + "." + digits[cut:]
	if negative {
		out = "-" + out
	}
	return out
}
