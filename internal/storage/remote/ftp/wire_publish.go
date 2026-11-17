package ftp

import (
	"context"
	"errors"
	"io"

	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func (s *wireSession) Rename(ctx context.Context, from, to string) error {
	if err := validateCommandArg(from); err != nil {
		return err
	}
	if err := validateCommandArg(to); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	code, msg, err := s.commandLocked(ctx, "RNFR "+from)
	if err != nil {
		return err
	}
	if code != 350 {
		return errors.Join(vfs.ErrPublishFailed, unexpectedCode("RNFR", code, msg, 350))
	}
	code, msg, err = s.commandLocked(ctx, "RNTO "+to)
	if err != nil {
		return err
	}
	if code != 250 {
		return errors.Join(vfs.ErrPublishFailed, unexpectedCode("RNTO", code, msg, 250))
	}
	return nil
}

// ReadTo performs one complete RETR without REST, including the final control
// response. It uses only the caller's managed buffer and never decodes bytes.
func (s *wireSession) ReadTo(ctx context.Context, path string, w io.Writer, b []byte) error {
	if err := validateCommandArg(path); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.openTransferLocked(ctx, "RETR "+path)
	if err != nil {
		return err
	}
	stop := armContext(ctx, data)
	defer stop()
	err = remote.CopyPhysical(ctx, data, w, b)
	_ = data.Close()
	finishErr := s.finishTransferLocked(ctx, err != nil)
	if err != nil {
		return err
	}
	return finishErr
}
