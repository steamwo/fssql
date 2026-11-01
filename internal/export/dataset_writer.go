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
	// manifest 是文件 PLANNED/WRITING/COMPLETE/FAILED 状态的唯一持久化来源。
	manifest *Manifest
	// router 负责 SingleFile/MultiFile/Partitioned 的稳定文件身份和行分组。
	router *FileRouter
	// root 是 Manifest Destination 解析后的数据集根 URI。
	root vfs.URI
	// open 经 FileWriterFactory/WriterPool/VFS 打开实际 writer；不得绕过 capability gate。
	open fileOpenFunc
	// single 仅在 SingleFile 模式跨 chunk 持有一个 bounded writer。
	single FormatWriter
	// singlePath 记录 single writer 对应的 Manifest 文件路径。
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

// newDatasetWriter 是测试注入点；生产路径必须由 NewDatasetWriter 传入 FileWriterFactory.Open。
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

// WriteChunk 按 route 写出一个稳定 segment；MultiFile/Partitioned 每个 route 完成即关闭，避免高基数分区长期占用 writer。
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

// writeRoute 把一个逻辑 route 的临时 DICTIONARY chunk 生命周期限制在单次文件写入内。
func (w *DatasetWriter) writeRoute(ctx context.Context, chunk *vector.DataChunk, route FileRoute) error {
	if err := w.manifest.PlanFile(route.Path); err != nil {
		return err
	}
	entry, _ := w.manifest.LookupFile(route.Path)
	if entry.State == FileStateComplete {
		// completed file 是 resume 粒度；稳定 segmentID 命中时不重复写入。
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

// Close 完成 SingleFile writer；多文件/分区模式每个 route 已在 WriteChunk 内完成，因此这里无额外远端资源。
func (w *DatasetWriter) Close(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if w.single == nil {
		return nil
	}
	result, err := w.single.Close(ctx)
	w.single = nil
	if err != nil {
		_ = w.manifest.TransitionFile(w.singlePath, FileStateFailed, 0, "")
		return err
	}
	return w.manifest.TransitionFile(w.singlePath, FileStateComplete, result.Bytes, result.Checksum)
}

// Abort 放弃仍打开的 SingleFile incomplete file，并把 Manifest 文件状态标记为 FAILED 供整文件重做。
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
	return w.manifest.TransitionFile(filePath, FileStateComplete, result.Bytes, result.Checksum)
}

// selectRouteChunk 用 DICTIONARY Vector 表达分区行选择，不复制底层 payload；临时 selection 最多一个 DataChunk 的 2048 行。
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

// fileURI 只在已有 Destination URI 下拼接受验证的相对 route path，不改变协议或 endpoint。
func (w *DatasetWriter) fileURI(filePath string) vfs.URI {
	uri := w.root
	uri.Path = pathpkg.Join(uri.Path, filePath)
	uri.Raw = uri.String()
	return uri
}
