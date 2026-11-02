package export

import (
	"context"
	"errors"
	"fmt"
	pathpkg "path"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// fileOpenFunc 是 DatasetWriter 对单文件 writer factory 的最小依赖，测试可注入故障实现。
type fileOpenFunc func(context.Context, vfs.URI, vfs.WriteOptions) (FormatWriter, error)

// DatasetWriter 把逻辑 FileRoute 顺序落到 bounded FileWriterFactory，并同步更新唯一 Manifest 状态源。
type DatasetWriter struct {
	manifest   *Manifest
	router     *FileRouter
	root       vfs.URI
	open       fileOpenFunc
	single     FormatWriter
	singlePath string
}

// NewDatasetWriter 校验 Manifest 已进入 WRITING，并把真实 FileWriterFactory 绑定为唯一打开路径。
func NewDatasetWriter(manifest *Manifest, router *FileRouter, factory *FileWriterFactory) (*DatasetWriter, error) {
	if factory == nil {
		return nil, errors.New("export: dataset writer requires file writer factory")
	}
	return newDatasetWriter(manifest, router, func(ctx context.Context, uri vfs.URI, opts vfs.WriteOptions) (FormatWriter, error) {
		return factory.Open(ctx, uri, opts)
	})
}

func newDatasetWriter(manifest *Manifest, router *FileRouter, open fileOpenFunc) (*DatasetWriter, error) {
	if manifest == nil || router == nil || open == nil {
		return nil, errors.New("export: dataset writer requires manifest, router and opener")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if manifest.ExportState != ExportStateWriting {
		return nil, fmt.Errorf("export: dataset writer requires WRITING manifest, got %s", manifest.ExportState)
	}
	root, err := vfs.ParseURI(manifest.Destination.URI)
	if err != nil {
		return nil, err
	}
	return &DatasetWriter{manifest: manifest, router: router, root: root, open: open}, nil
}

func (w *DatasetWriter) WriteChunk(ctx context.Context, chunk *vector.DataChunk, segmentID string) error {
	if w == nil || w.manifest == nil || w.router == nil {
		return errors.New("export: incomplete dataset writer")
	}
	routes, err := w.router.RouteChunk(chunk, segmentID)
	if err != nil {
		return err
	}
	for _, route := range routes {
		if err := w.writeRoute(ctx, chunk, route); err != nil {
			return err
		}
	}
	return nil
}

func (w *DatasetWriter) writeRoute(ctx context.Context, chunk *vector.DataChunk, route FileRoute) error {
	if err := w.manifest.PlanFile(route.Path); err != nil {
		return err
	}
	entry, _ := w.manifest.LookupFile(route.Path)
	if entry.State == FileStateComplete {
		return nil
	}
	selected, release, err := selectRouteChunk(chunk, route.Rows)
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}
	if w.router.config.Mode == LayoutSingleFile {
		return w.writeSingle(ctx, route.Path, selected)
	}
	return w.writeCompleteFile(ctx, route.Path, selected)
}

func (w *DatasetWriter) Close(ctx context.Context) error {
	if w == nil || w.single == nil {
		return nil
	}
	result, err := w.single.Close(ctx)
	w.single = nil
	if err != nil {
		_ = w.manifest.TransitionFile(w.singlePath, FileStateFailed, 0, "")
		return err
	}
	return w.completeFile(w.singlePath, result)
}

func (w *DatasetWriter) Abort(ctx context.Context) error {
	if w == nil || w.single == nil {
		return nil
	}
	err := w.single.Abort(ctx)
	w.single = nil
	stateErr := w.manifest.TransitionFile(w.singlePath, FileStateFailed, 0, "")
	return errors.Join(err, stateErr)
}

func (w *DatasetWriter) writeSingle(ctx context.Context, filePath string, chunk *vector.DataChunk) error {
	if w.single == nil {
		if err := w.manifest.TransitionFile(filePath, FileStateWriting, 0, ""); err != nil {
			return err
		}
		writer, err := w.open(ctx, w.fileURI(filePath), vfs.WriteOptions{})
		if err != nil {
			_ = w.manifest.TransitionFile(filePath, FileStateFailed, 0, "")
			return err
		}
		w.single = writer
		w.singlePath = filePath
	}
	if err := w.single.WriteChunk(ctx, chunk); err != nil {
		abortErr := w.single.Abort(context.Background())
		w.single = nil
		stateErr := w.manifest.TransitionFile(filePath, FileStateFailed, 0, "")
		return errors.Join(err, abortErr, stateErr)
	}
	return nil
}

func (w *DatasetWriter) writeCompleteFile(ctx context.Context, filePath string, chunk *vector.DataChunk) error {
	if err := w.manifest.TransitionFile(filePath, FileStateWriting, 0, ""); err != nil {
		return err
	}
	writer, err := w.open(ctx, w.fileURI(filePath), vfs.WriteOptions{})
	if err != nil {
		_ = w.manifest.TransitionFile(filePath, FileStateFailed, 0, "")
		return err
	}
	if err := writer.WriteChunk(ctx, chunk); err != nil {
		abortErr := writer.Abort(context.Background())
		stateErr := w.manifest.TransitionFile(filePath, FileStateFailed, 0, "")
		return errors.Join(err, abortErr, stateErr)
	}
	result, err := writer.Close(ctx)
	if err != nil {
		abortErr := writer.Abort(context.Background())
		stateErr := w.manifest.TransitionFile(filePath, FileStateFailed, 0, "")
		return errors.Join(err, abortErr, stateErr)
	}
	return w.completeFile(filePath, result)
}

// completeFile keeps legacy dataset writers compatible while persisting stable
// VFS identity whenever the driver provides one. Single-file commit later
// requires this identity; MultiFile/Partitioned commit semantics stay unchanged.
func (w *DatasetWriter) completeFile(filePath string, result WriteResult) error {
	if result.PublishedIdentity != nil {
		return w.manifest.TransitionFilePublished(filePath, result.Bytes, result.Checksum, *result.PublishedIdentity)
	}
	return w.manifest.TransitionFile(filePath, FileStateComplete, result.Bytes, result.Checksum)
}

func selectRouteChunk(chunk *vector.DataChunk, rows []uint32) (*vector.DataChunk, func(), error) {
	if rows == nil {
		return chunk, nil, nil
	}
	selection := vector.SelectionFrom(rows)
	columns := make([]*vector.Vector, 0, len(chunk.Columns))
	for _, child := range chunk.Columns {
		dictionary, err := vector.NewDictionary(child, selection)
		if err != nil {
			for _, column := range columns {
				column.Release()
			}
			return nil, nil, err
		}
		columns = append(columns, dictionary)
	}
	selected, err := vector.NewDataChunk(columns...)
	if err != nil {
		for _, column := range columns {
			column.Release()
		}
		return nil, nil, err
	}
	return selected, selected.Release, nil
}

func (w *DatasetWriter) fileURI(filePath string) vfs.URI {
	uri := w.root
	uri.Path = pathpkg.Join(uri.Path, filePath)
	uri.Raw = uri.String()
	return uri
}
