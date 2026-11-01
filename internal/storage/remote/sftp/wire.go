package sftp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	fxpInit         = 1
	fxpVersion      = 2
	fxpOpen         = 3
	fxpClose        = 4
	fxpRead         = 5
	fxpOpendir      = 11
	fxpReaddir      = 12
	fxpStat         = 17
	fxpStatus       = 101
	fxpHandle       = 102
	fxpData         = 103
	fxpName         = 104
	fxpAttrs        = 105
	fxRead          = 1
	fxOK            = 0
	fxEOF           = 1
	attrSize        = 1
	attrUIDGID      = 2
	attrPermissions = 4
	attrACModTime   = 8
	attrExtended    = 0x80000000
)

type wireSession struct {
	rw        io.ReadWriteCloser
	opMu      sync.Mutex
	closeOnce sync.Once
	nextID    uint32
}

func NewWireSession(rw io.ReadWriteCloser) (Session, error) {
	if rw == nil {
		return nil, errors.New("sftp: nil transport")
	}
	s := &wireSession{rw: rw, nextID: 1}
	if err := s.handshake(); err != nil {
		_ = rw.Close()
		return nil, err
	}
	return s, nil
}
func (s *wireSession) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.rw.Close() })
	return err
}
func (s *wireSession) withContext(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = s.Close() })
	defer stop()
	err := fn()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
func (s *wireSession) handshake() error {
	var payload [4]byte
	binary.BigEndian.PutUint32(payload[:], 3)
	if err := s.writePacket(fxpInit, payload[:]); err != nil {
		return err
	}
	typ, data, err := s.readPacket()
	if err != nil {
		return err
	}
	if typ != fxpVersion || len(data) < 4 {
		return fmt.Errorf("sftp: invalid VERSION packet")
	}
	version := binary.BigEndian.Uint32(data[:4])
	if version < 3 {
		return fmt.Errorf("sftp: server version %d < 3", version)
	}
	return nil
}
func (s *wireSession) next() uint32 {
	id := s.nextID
	s.nextID++
	if s.nextID == 0 {
		s.nextID = 1
	}
	return id
}
func (s *wireSession) writePacket(typ byte, payload []byte) error {
	n := 1 + len(payload)
	buf := make([]byte, 4+n)
	binary.BigEndian.PutUint32(buf[:4], uint32(n))
	buf[4] = typ
	copy(buf[5:], payload)
	if _, err := io.Copy(s.rw, bytesReader(buf)); err != nil {
		return fmt.Errorf("%w: write packet: %v", ErrConnection, err)
	}
	return nil
}
func (s *wireSession) readPacket() (byte, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(s.rw, lenBuf[:]); err != nil {
		return 0, nil, fmt.Errorf("%w: read packet length: %v", ErrConnection, err)
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n < 1 || n > 128<<10 {
		return 0, nil, fmt.Errorf("sftp: invalid packet length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(s.rw, buf); err != nil {
		return 0, nil, fmt.Errorf("%w: read packet: %v", ErrConnection, err)
	}
	return buf[0], buf[1:], nil
}

type sliceReader struct{ b []byte }

func bytesReader(b []byte) *sliceReader { return &sliceReader{b: b} }
func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

func appendU32(b []byte, v uint32) []byte {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], v)
	return append(b, x[:]...)
}
func appendU64(b []byte, v uint64) []byte {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], v)
	return append(b, x[:]...)
}
func appendString(b []byte, s string) []byte {
	b = appendU32(b, uint32(len(s)))
	return append(b, s...)
}
func appendBytes(b []byte, v []byte) []byte { b = appendU32(b, uint32(len(v))); return append(b, v...) }

type parser struct {
	b   []byte
	off int
}

func (p *parser) u32() (uint32, error) {
	if p.off+4 > len(p.b) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(p.b[p.off : p.off+4])
	p.off += 4
	return v, nil
}
func (p *parser) u64() (uint64, error) {
	if p.off+8 > len(p.b) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(p.b[p.off : p.off+8])
	p.off += 8
	return v, nil
}
func (p *parser) bytes() ([]byte, error) {
	n, err := p.u32()
	if err != nil {
		return nil, err
	}
	if uint64(p.off)+uint64(n) > uint64(len(p.b)) {
		return nil, io.ErrUnexpectedEOF
	}
	v := p.b[p.off : p.off+int(n)]
	p.off += int(n)
	return v, nil
}
func (p *parser) str() (string, error) { b, err := p.bytes(); return string(b), err }

func (s *wireSession) request(typ byte, payload []byte) (byte, []byte, uint32, error) {
	id := s.next()
	body := appendU32(nil, id)
	body = append(body, payload...)
	if err := s.writePacket(typ, body); err != nil {
		return 0, nil, id, err
	}
	rt, data, err := s.readPacket()
	if err != nil {
		return 0, nil, id, err
	}
	if len(data) < 4 {
		return 0, nil, id, fmt.Errorf("sftp: response missing id")
	}
	rid := binary.BigEndian.Uint32(data[:4])
	if rid != id {
		return 0, nil, id, fmt.Errorf("sftp: response id %d want %d", rid, id)
	}
	return rt, data[4:], id, nil
}
func parseStatus(data []byte) error {
	p := parser{b: data}
	code, err := p.u32()
	if err != nil {
		return err
	}
	msg, _ := p.str()
	switch code {
	case fxOK:
		return nil
	case fxEOF:
		return io.EOF
	default:
		return fmt.Errorf("sftp: status %d: %s", code, msg)
	}
}
func parseAttrs(data []byte) (FileInfo, error) {
	p := parser{b: data}
	flags, err := p.u32()
	if err != nil {
		return FileInfo{}, err
	}
	var out FileInfo
	if flags&attrSize != 0 {
		v, err := p.u64()
		if err != nil {
			return out, err
		}
		out.Size = int64(v)
	}
	if flags&attrUIDGID != 0 {
		if _, err := p.u32(); err != nil {
			return out, err
		}
		if _, err := p.u32(); err != nil {
			return out, err
		}
	}
	if flags&attrPermissions != 0 {
		perm, err := p.u32()
		if err != nil {
			return out, err
		}
		out.IsDir = perm&0040000 != 0
	}
	if flags&attrACModTime != 0 {
		if _, err := p.u32(); err != nil {
			return out, err
		}
		mt, err := p.u32()
		if err != nil {
			return out, err
		}
		out.ModUnix = int64(mt)
	}
	if flags&attrExtended != 0 {
		n, err := p.u32()
		if err != nil {
			return out, err
		}
		for i := uint32(0); i < n; i++ {
			if _, err := p.bytes(); err != nil {
				return out, err
			}
			if _, err := p.bytes(); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

func (s *wireSession) Stat(ctx context.Context, path string) (out FileInfo, err error) {
	err = s.withContext(ctx, func() error {
		typ, data, _, err := s.request(fxpStat, appendString(nil, path))
		if err != nil {
			return err
		}
		if typ == fxpStatus {
			return parseStatus(data)
		}
		if typ != fxpAttrs {
			return fmt.Errorf("sftp: STAT response type %d", typ)
		}
		a, err := parseAttrs(data)
		out = a
		return err
	})
	return
}
func (s *wireSession) open(path string) ([]byte, error) {
	payload := appendString(nil, path)
	payload = appendU32(payload, fxRead)
	payload = appendU32(payload, 0)
	typ, data, _, err := s.request(fxpOpen, payload)
	if err != nil {
		return nil, err
	}
	if typ == fxpStatus {
		return nil, parseStatus(data)
	}
	if typ != fxpHandle {
		return nil, fmt.Errorf("sftp: OPEN response %d", typ)
	}
	p := parser{b: data}
	h, err := p.bytes()
	return append([]byte(nil), h...), err
}
func (s *wireSession) closeHandle(h []byte) error {
	typ, data, _, err := s.request(fxpClose, appendBytes(nil, h))
	if err != nil {
		return err
	}
	if typ != fxpStatus {
		return fmt.Errorf("sftp: CLOSE response %d", typ)
	}
	return parseStatus(data)
}
func (s *wireSession) ReadAt(ctx context.Context, path string, dst []byte, offset int64) (n int, err error) {
	if offset < 0 {
		return 0, fmt.Errorf("sftp: negative offset")
	}
	err = s.withContext(ctx, func() error {
		h, err := s.open(path)
		if err != nil {
			return err
		}
		defer func() { _ = s.closeHandle(h) }()
		for n < len(dst) {
			want := len(dst) - n
			if want > 1<<20 {
				want = 1 << 20
			}
			payload := appendBytes(nil, h)
			payload = appendU64(payload, uint64(offset)+uint64(n))
			payload = appendU32(payload, uint32(want))
			typ, data, _, err := s.request(fxpRead, payload)
			if err != nil {
				return err
			}
			if typ == fxpStatus {
				e := parseStatus(data)
				if errors.Is(e, io.EOF) {
					return io.EOF
				}
				return e
			}
			if typ != fxpData {
				return fmt.Errorf("sftp: READ response %d", typ)
			}
			p := parser{b: data}
			chunk, err := p.bytes()
			if err != nil {
				return err
			}
			if len(chunk) == 0 {
				return io.EOF
			}
			copied := copy(dst[n:], chunk)
			n += copied
			if copied < len(chunk) {
				return fmt.Errorf("sftp: server returned oversized READ data")
			}
		}
		return nil
	})
	if errors.Is(err, io.EOF) && n > 0 {
		return n, io.EOF
	}
	return n, err
}
func (s *wireSession) List(ctx context.Context, path string) (out []DirEntry, err error) {
	err = s.withContext(ctx, func() error {
		typ, data, _, err := s.request(fxpOpendir, appendString(nil, path))
		if err != nil {
			return err
		}
		if typ == fxpStatus {
			return parseStatus(data)
		}
		if typ != fxpHandle {
			return fmt.Errorf("sftp: OPENDIR response %d", typ)
		}
		p := parser{b: data}
		h, err := p.bytes()
		if err != nil {
			return err
		}
		defer func() { _ = s.closeHandle(h) }()
		for {
			typ, data, _, err = s.request(fxpReaddir, appendBytes(nil, h))
			if err != nil {
				return err
			}
			if typ == fxpStatus {
				e := parseStatus(data)
				if errors.Is(e, io.EOF) {
					return nil
				}
				return e
			}
			if typ != fxpName {
				return fmt.Errorf("sftp: READDIR response %d", typ)
			}
			pp := parser{b: data}
			count, err := pp.u32()
			if err != nil {
				return err
			}
			for i := uint32(0); i < count; i++ {
				name, err := pp.str()
				if err != nil {
					return err
				}
				if _, err := pp.str(); err != nil {
					return err
				}
				info, err := parseAttrs(pp.b[pp.off:])
				if err != nil {
					return err
				}
				attrParser := parser{b: pp.b[pp.off:]}
				flags, err := attrParser.u32()
				if err != nil {
					return err
				}
				if flags&attrSize != 0 {
					_, _ = attrParser.u64()
				}
				if flags&attrUIDGID != 0 {
					_, _ = attrParser.u32()
					_, _ = attrParser.u32()
				}
				if flags&attrPermissions != 0 {
					_, _ = attrParser.u32()
				}
				if flags&attrACModTime != 0 {
					_, _ = attrParser.u32()
					_, _ = attrParser.u32()
				}
				if flags&attrExtended != 0 {
					cnt, _ := attrParser.u32()
					for j := uint32(0); j < cnt; j++ {
						_, _ = attrParser.bytes()
						_, _ = attrParser.bytes()
					}
				}
				pp.off += attrParser.off
				if name != "." && name != ".." {
					out = append(out, DirEntry{Name: name, Info: info})
				}
			}
		}
	})
	return
}
