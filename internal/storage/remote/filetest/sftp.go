package filetest

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
)

func u32(b []byte, n uint32) []byte {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], n)
	return append(b, x[:]...)
}
func str(b []byte, s string) []byte { return append(u32(b, uint32(len(s))), s...) }

type packet struct {
	b   []byte
	off int
}

func (p *packet) number() uint32 {
	if p.off+4 > len(p.b) {
		return 0
	}
	n := binary.BigEndian.Uint32(p.b[p.off:])
	p.off += 4
	return n
}
func (p *packet) text() string {
	n := int(p.number())
	if n > len(p.b)-p.off {
		return ""
	}
	s := string(p.b[p.off : p.off+n])
	p.off += n
	return s
}
func readPacket(c io.Reader) (byte, []byte, error) {
	var h [4]byte
	if _, e := io.ReadFull(c, h[:]); e != nil {
		return 0, nil, e
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 || n > 1<<20 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	b := make([]byte, n)
	_, e := io.ReadFull(c, b)
	return b[0], b[1:], e
}
func writePacket(c io.Writer, t byte, b []byte) {
	_, _ = c.Write(append(append(u32(nil, uint32(len(b)+1)), t), b...))
}
func status(c io.Writer, id, code uint32) {
	writePacket(c, 101, str(str(u32(u32(nil, id), code), "fixture"), ""))
}

func (s *Store) SFTP(c net.Conn) {
	t, _, err := readPacket(c)
	if err != nil || t != 1 {
		return
	}
	s.Mu.Lock()
	link := s.Hardlink
	s.Mu.Unlock()
	b := u32(nil, 3)
	if link != "" {
		b = str(str(b, "hardlink@openssh.com"), link)
	}
	writePacket(c, 2, b)
	for {
		t, b, err = readPacket(c)
		if err != nil {
			return
		}
		p := packet{b: b}
		id := p.number()
		switch t {
		case 3:
			name := p.text()
			flags := p.number()
			_, exists := s.Get(name)
			if flags&32 != 0 && exists {
				status(c, id, 4)
				continue
			}
			if flags&8 != 0 {
				s.Put(name, nil)
				s.event("create")
				if s.fault("create_lost") {
					return
				}
			} else if !exists {
				status(c, id, 2)
				continue
			}
			if flags&16 != 0 {
				s.Put(name, nil)
			}
			writePacket(c, 102, str(u32(nil, id), name))
		case 4:
			status(c, id, 0)
		case 5:
			name := p.text()
			hi, lo := p.number(), p.number()
			off := int64(hi)<<32 | int64(lo)
			want := int(p.number())
			data, exists := s.Get(name)
			if s.fault("read_error") && !strings.Contains(name, ".fssql-") {
				return
			}
			if !exists {
				status(c, id, 2)
				continue
			}
			if off >= int64(len(data)) {
				status(c, id, 1)
				continue
			}
			end := int(off) + want
			if end > len(data) {
				end = len(data)
			}
			writePacket(c, 103, str(u32(nil, id), string(data[off:end])))
		case 6:
			name := p.text()
			hi, lo := p.number(), p.number()
			off := int64(hi)<<32 | int64(lo)
			data := p.text()
			old, exists := s.Get(name)
			if !exists {
				status(c, id, 2)
				continue
			}
			if int(off)+len(data) > len(old) {
				old = append(old, make([]byte, int(off)+len(data)-len(old))...)
			}
			copy(old[off:], data)
			s.Put(name, old)
			s.event("write")
			status(c, id, 0)
		case 17:
			name := p.text()
			data, exists := s.Get(name)
			if !exists {
				status(c, id, 2)
				continue
			}
			b = u32(u32(nil, id), 1|8)
			b = u32(u32(b, uint32(uint64(len(data))>>32)), uint32(len(data)))
			b = u32(u32(b, 1234), 1234)
			writePacket(c, 105, b)
		case 13:
			name := p.text()
			s.event("remove")
			if s.fault("cleanup_error") {
				status(c, id, 4)
				continue
			}
			s.Remove(name)
			status(c, id, 0)
		case 18, 200:
			if t == 200 {
				if p.text() != "hardlink@openssh.com" {
					status(c, id, 8)
					continue
				}
				s.event("hardlink")
			} else {
				s.event("rename")
			}
			from, to := p.text(), p.text()
			s.before()
			if s.fault("publish_before") {
				return
			}
			s.Mu.Lock()
			data, exists := s.Files[from]
			_, conflict := s.Files[to]
			if !exists || conflict {
				s.Mu.Unlock()
				status(c, id, 4)
				continue
			}
			s.Files[to] = data
			if t == 18 {
				delete(s.Files, from)
			}
			s.Mu.Unlock()
			if s.fault("publish_lost") {
				return
			}
			status(c, id, 0)
		default:
			status(c, id, 8)
		}
	}
}
