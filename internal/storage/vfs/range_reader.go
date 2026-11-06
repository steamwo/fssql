package vfs

import (
	"context"

	"github.com/steamwo/fssql/internal/core/memory"
)

// RangeReader 在一次扫描中复用对象句柄；调用方负责关闭，读取仍遵守精确范围与身份校验。
type RangeReader interface {
	// ReadRange 按受管预算读取精确范围，并验证请求的对象身份。
	ReadRange(context.Context, RangeRequest, *memory.Scope, *memory.BufferManager) (RangeResult, error)
	// Close 释放句柄；调用方必须先结束所有读取。
	Close() error
}

// RangeReaderOpener 是可选的句柄复用能力，未实现的 Driver 保留逐次 ReadRange 路径。
type RangeReaderOpener interface {
	// OpenRangeReader 打开单对象会话，身份不匹配时返回错误。
	OpenRangeReader(context.Context, URI, *ObjectIdentity) (RangeReader, error)
}
