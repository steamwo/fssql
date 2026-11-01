package sftp

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
)

// serveSFTPWrite 是最小 v3 server，验证 OPEN flags、WRITE offset/payload、CLOSE 与 REMOVE 报文。
func serveSFTPWrite(t *testing.T, c net.Conn, written chan<- []byte, removed chan<- string) {
	t.Helper()
	defer c.Close()
	typ, p, err := srvRead(c)
	if err != nil || typ != fxpInit || len(p) < 4 || binary.BigEndian.Uint32(p[:4]) != 3 {
		t.Errorf("bad init typ=%d err=%v", typ, err)
		return
	}
	_ = srvWrite(c, fxpVersion, appendU32(nil, 3))
	file := []byte(nil)
	for {
		typ, p, err = srvRead(c)
		if err != nil {
			return
		}
		if len(p) < 4 {
			t.Errorf("short request")
			return
		}
		id := binary.BigEndian.Uint32(p[:4])
		body := p[4:]
		switch typ {
		case fxpOpen:
			pp := parser{b: body}
			_, _ = pp.str()
			flags, parseErr := pp.u32()
			if parseErr != nil || flags&fxWrite == 0 || flags&fxCreat == 0 {
				t.Errorf("bad write OPEN flags=%x err=%v", flags, parseErr)
				return
			}
			if flags&fxTrunc != 0 {
				file = nil
			}
			b := appendU32(nil, id)
			b = appendBytes(b, []byte("wh"))
			_ = srvWrite(c, fxpHandle, b)
		case fxpWrite:
			pp := parser{b: body}
			_, _ = pp.bytes()
			off, _ := pp.u64()
			data, parseErr := pp.bytes()
			if parseErr != nil {
				t.Errorf("parse WRITE: %v", parseErr)
				return
			}
			need := int(off) + len(data)
			if need > len(file) {
				next := make([]byte, need)
				copy(next, file)
				file = next
			}
			copy(file[int(off):], data)
			// 先记录观测值再发送 response，确保 WriteAt 返回时测试一定能读到本次写入结果。
			written <- append([]byte(nil), file...)
			_ = srvWrite(c, fxpStatus, statusPayload(id, fxOK))
		case fxpClose:
			_ = srvWrite(c, fxpStatus, statusPayload(id, fxOK))
		case fxpRemove:
			pp := parser{b: body}
			name, parseErr := pp.str()
			if parseErr != nil {
				t.Errorf("parse REMOVE: %v", parseErr)
				return
			}
			removed <- name
			file = nil
			_ = srvWrite(c, fxpStatus, statusPayload(id, fxOK))
		default:
			t.Errorf("unexpected request %d", typ)
			return
		}
	}
}

// TestWireSessionWriteAtAndRemove 验证真正 wireSession 实现 WritableSession，而不是仅 driver mock 可写。
func TestWireSessionWriteAtAndRemove(t *testing.T) {
	client, server := net.Pipe()
	written := make(chan []byte, 2)
	removed := make(chan string, 1)
	go serveSFTPWrite(t, server, written, removed)
	sessI, err := NewWireSession(client)
	if err != nil {
		t.Fatal(err)
	}
	ws, ok := sessI.(WritableSession)
	if !ok {
		t.Fatal("wire session does not implement WritableSession")
	}
	if n, err := ws.WriteAt(context.Background(), "/out", []byte("abc"), 0, true); err != nil || n != 3 {
		t.Fatalf("write1 n=%d err=%v", n, err)
	}
	if n, err := ws.WriteAt(context.Background(), "/out", []byte("def"), 3, false); err != nil || n != 3 {
		t.Fatalf("write2 n=%d err=%v", n, err)
	}
	<-written
	latest := <-written
	if string(latest) != "abcdef" {
		t.Fatalf("wire data=%q", latest)
	}
	if err := ws.Remove(context.Background(), "/out"); err != nil {
		t.Fatal(err)
	}
	select {
	case name := <-removed:
		if name != "/out" {
			t.Fatalf("removed=%q", name)
		}
	default:
		t.Fatal("REMOVE request not observed")
	}
	_ = ws.Close()
}

// TestWireSessionWriteCancellation 验证调用前已取消时不会产生 WRITE 请求。
func TestWireSessionWriteCancellation(t *testing.T) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		typ, _, err := srvRead(server)
		if err == nil && typ == fxpInit {
			_ = srvWrite(server, fxpVersion, appendU32(nil, 3))
		}
		_, _, _ = srvRead(server)
	}()
	sessI, err := NewWireSession(client)
	if err != nil {
		t.Fatal(err)
	}
	ws := sessI.(WritableSession)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ws.WriteAt(ctx, "/out", []byte("x"), 0, true); err != context.Canceled {
		t.Fatalf("expected cancel, got %v", err)
	}
	_ = ws.Close()
}
