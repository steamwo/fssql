package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
)

const (
	// fxpWrite/fxpRemove 是 SFTP v3 文件写入和删除请求类型。
	fxpWrite  = 6
	fxpRemove = 13
	// fxWrite/fxCreat/fxTrunc 是 SSH_FXP_OPEN 的 v3 pflags。
	fxWrite = 2
	fxCreat = 8
	fxTrunc = 16
	// maxWriteChunk 保证 WRITE packet 始终低于 wire reader 的 128KiB 上限。
	maxWriteChunk = 32 << 10
)

// openWriteHandle 打开一个可写 handle；truncate 只用于 incomplete-file 新尝试的第一次写入。
func (s *wireSession) openWriteHandle(path string, truncate bool) ([]byte, error) {
	flags := uint32(fxWrite | fxCreat)
	if truncate {
		flags |= fxTrunc
	}
	return s.openWriteFlags(path, flags)
}

func (s *wireSession) openWriteFlags(path string, flags uint32) ([]byte, error) {
	payload := appendString(nil, path)
	payload = appendU32(payload, flags)
	// v3 OPEN 的 attrs 不能为空；flags=0 表示不额外设置属性。
	payload = appendU32(payload, 0)
	typ, data, _, err := s.request(fxpOpen, payload)
	if err != nil {
		return nil, err
	}
	if typ == fxpStatus {
		return nil, parseStatus(data)
	}
	if typ != fxpHandle {
		return nil, fmt.Errorf("sftp: OPEN(write) response %d", typ)
	}
	p := parser{b: data}
	h, err := p.bytes()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), h...), nil
}

// WriteAt 实现 WritableSession；每次调用只持有 bounded protocol packet，caller data 不被 session 长期保留。
func (s *wireSession) WriteAt(ctx context.Context, path string, data []byte, offset int64, truncate bool) (n int, err error) {
	flags := uint32(fxWrite | fxCreat)
	if truncate {
		flags |= fxTrunc
	}
	return s.writeAtFlags(ctx, path, data, offset, flags)
}

func (s *wireSession) writeAtFlags(ctx context.Context, path string, data []byte, offset int64, flags uint32) (n int, err error) {
	if offset < 0 {
		return 0, errors.New("sftp: negative write offset")
	}
	err = s.withContext(ctx, func() error {
		h, err := s.openWriteFlags(path, flags)
		if err != nil {
			return err
		}
		closeNeeded := true
		defer func() {
			if closeNeeded {
				_ = s.closeHandle(h)
			}
		}()
		for n < len(data) {
			if err := ctx.Err(); err != nil {
				return err
			}
			want := len(data) - n
			if want > maxWriteChunk {
				want = maxWriteChunk
			}
			payload := appendBytes(nil, h)
			payload = appendU64(payload, uint64(offset)+uint64(n))
			payload = appendBytes(payload, data[n:n+want])
			typ, response, _, err := s.request(fxpWrite, payload)
			if err != nil {
				return err
			}
			if typ != fxpStatus {
				return fmt.Errorf("sftp: WRITE response %d", typ)
			}
			if err := parseStatus(response); err != nil {
				return err
			}
			n += want
		}
		if err := s.closeHandle(h); err != nil {
			return err
		}
		closeNeeded = false
		return nil
	})
	if err != nil && n > 0 && errors.Is(err, io.EOF) {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// Remove 实现 WritableSession 的 incomplete/staging 文件删除。
func (s *wireSession) Remove(ctx context.Context, path string) error {
	return s.withContext(ctx, func() error {
		typ, data, _, err := s.request(fxpRemove, appendString(nil, path))
		if err != nil {
			return err
		}
		if typ != fxpStatus {
			return fmt.Errorf("sftp: REMOVE response %d", typ)
		}
		return parseStatus(data)
	})
}
