package sftp

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

func srvRead(r io.Reader) (byte, []byte, error) {
	var lb [4]byte
	if _, e := io.ReadFull(r, lb[:]); e != nil { return 0, nil, e }
	n := binary.BigEndian.Uint32(lb[:]); b := make([]byte, n)
	if _, e := io.ReadFull(r, b); e != nil { return 0, nil, e }
	return b[0], b[1:], nil
}
func srvWrite(w io.Writer, typ byte, payload []byte) error {
	n := 1 + len(payload); b := make([]byte, 4+n); binary.BigEndian.PutUint32(b[:4], uint32(n)); b[4] = typ; copy(b[5:], payload); _, e := w.Write(b); return e
}
func statusPayload(id, code uint32) []byte { b := appendU32(nil, id); b = appendU32(b, code); b = appendString(b, ""); b = appendString(b, ""); return b }
func attrsPayload(id uint32, size uint64, dir bool) []byte {
	b := appendU32(nil, id); b = appendU32(b, attrSize|attrPermissions|attrACModTime); b = appendU64(b, size)
	perm := uint32(0100644); if dir { perm = 0040755 }; b = appendU32(b, perm); b = appendU32(b, 1000); b = appendU32(b, 1234); return b
}
func serveSFTP(t *testing.T, c net.Conn, data []byte) {
	t.Helper(); defer c.Close()
	typ, p, err := srvRead(c); if err != nil || typ != fxpInit { t.Errorf("init typ=%d err=%v", typ, err); return }
	if len(p) < 4 || binary.BigEndian.Uint32(p[:4]) != 3 { t.Errorf("bad init") }
	_ = srvWrite(c, fxpVersion, appendU32(nil, 3)); dirRead := false
	for {
		typ, p, err = srvRead(c); if err != nil { return }; if len(p) < 4 { t.Errorf("short request"); return }
		id := binary.BigEndian.Uint32(p[:4]); body := p[4:]
		switch typ {
		case fxpStat:
			_ = body; _ = srvWrite(c, fxpAttrs, attrsPayload(id, uint64(len(data)), false))
		case fxpOpen:
			b := appendU32(nil, id); b = appendBytes(b, []byte("h")); _ = srvWrite(c, fxpHandle, b)
		case fxpRead:
			pp := parser{b: body}; _, _ = pp.bytes(); off, _ := pp.u64(); want, _ := pp.u32()
			if off >= uint64(len(data)) { _ = srvWrite(c, fxpStatus, statusPayload(id, fxEOF)); continue }
			end := int(off) + int(want); if end > len(data) { end = len(data) }; b := appendU32(nil, id); b = appendBytes(b, data[int(off):end]); _ = srvWrite(c, fxpData, b)
		case fxpClose:
			_ = srvWrite(c, fxpStatus, statusPayload(id, fxOK))
		case fxpOpendir:
			b := appendU32(nil, id); b = appendBytes(b, []byte("d")); _ = srvWrite(c, fxpHandle, b)
		case fxpReaddir:
			if dirRead { _ = srvWrite(c, fxpStatus, statusPayload(id, fxEOF)); continue }; dirRead = true
			b := appendU32(nil, id); b = appendU32(b, 1); b = appendString(b, "data.csv"); b = appendString(b, "data.csv"); attrs := attrsPayload(0, uint64(len(data)), false)[4:]; b = append(b, attrs...); _ = srvWrite(c, fxpName, b)
		default: t.Errorf("unexpected request %d", typ); return
		}
	}
}
func TestWireSessionStatReadList(t *testing.T) {
	client, server := net.Pipe(); data := []byte("abcdef"); go serveSFTP(t, server, data)
	sessI, err := NewWireSession(client); if err != nil { t.Fatal(err) }; sess := sessI.(*wireSession)
	info, err := sess.Stat(context.Background(), "/data.csv"); if err != nil || info.Size != 6 || info.ModUnix != 1234 { t.Fatalf("stat=%+v err=%v", info, err) }
	buf := make([]byte, 3); n, err := sess.ReadAt(context.Background(), "/data.csv", buf, 2); if err != nil || n != 3 || string(buf) != "cde" { t.Fatalf("read n=%d data=%q err=%v", n, buf, err) }
	entries, err := sess.List(context.Background(), "/"); if err != nil || len(entries) != 1 || entries[0].Name != "data.csv" || entries[0].Info.Size != 6 { t.Fatalf("entries=%+v err=%v", entries, err) }
	_ = sess.Close()
}
