package sftp

import (
	"context"
	"fmt"
)

func (s *wireSession) PublishExtensions() (uint32, string) { return s.version, s.hardlinkVersion }

func (s *wireSession) CreateExclusive(ctx context.Context, path string) error {
	return s.withContext(ctx, func() error {
		payload := appendString(nil, path)
		payload = appendU32(payload, fxWrite|fxCreat|32)
		payload = appendU32(payload, 0)
		typ, b, _, err := s.request(fxpOpen, payload)
		if err != nil {
			return err
		}
		if typ == fxpStatus {
			return parseStatus(b)
		}
		if typ != fxpHandle {
			return fmt.Errorf("sftp: exclusive OPEN response %d", typ)
		}
		p := parser{b: b}
		h, err := p.bytes()
		if err != nil {
			return err
		}
		return s.closeHandle(h)
	})
}

func (s *wireSession) WriteExisting(ctx context.Context, path string, b []byte, off int64) (int, error) {
	return s.writeAtFlags(ctx, path, b, off, fxWrite)
}

type publishStatusError struct{ err error }

func (e *publishStatusError) Error() string { return e.err.Error() }
func (e *publishStatusError) Unwrap() error { return e.err }

func (s *wireSession) publishRequest(ctx context.Context, typ byte, payload []byte) error {
	return s.withContext(ctx, func() error {
		rt, b, _, err := s.request(typ, payload)
		if err != nil {
			return err
		}
		if rt != fxpStatus {
			return fmt.Errorf("sftp: publish response %d", rt)
		}
		if err = parseStatus(b); err != nil {
			return &publishStatusError{err}
		}
		return nil
	})
}
func (s *wireSession) Rename(ctx context.Context, from, to string) error {
	return s.publishRequest(ctx, 18, appendString(appendString(nil, from), to))
}
func (s *wireSession) Hardlink(ctx context.Context, from, to string) error {
	if s.hardlinkVersion != "1" {
		return fmt.Errorf("sftp: hardlink extension unavailable")
	}
	return s.publishRequest(ctx, 200, appendString(appendString(appendString(nil, "hardlink@openssh.com"), from), to))
}
