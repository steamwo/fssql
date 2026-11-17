package filetest

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"path"
	"strconv"
	"strings"
)

func (s *Store) FTP(c net.Conn, tlsConfig *tls.Config) {
	if tlsConfig != nil {
		conn := tls.Server(c, tlsConfig)
		if conn.Handshake() != nil {
			return
		}
		c = conn
	}
	reader := bufio.NewReader(c)
	reply := func(format string, args ...any) { _, _ = fmt.Fprintf(c, format+"\r\n", args...) }
	reply("220 fixture")
	var passive net.Listener
	defer func() {
		if passive != nil {
			_ = passive.Close()
		}
	}()
	var rest int64
	var rename string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		op, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
		s.event(op)
		switch op {
		case "USER":
			if arg != "user" {
				reply("530 login denied")
			} else {
				reply("331 password")
			}
		case "PASS":
			if arg != "pass" {
				reply("530 login denied")
			} else {
				reply("230 logged in")
			}
		case "TYPE", "PBSZ", "PROT":
			reply("200 ok")
		case "FEAT":
			reply("211-Features\r\n REST STREAM\r\n MLST type*;size*;modify*;\r\n211 End")
		case "SIZE":
			b, ok := s.Get(arg)
			if !ok {
				reply("550 unavailable")
			} else {
				reply("213 %d", len(b))
			}
		case "MDTM":
			reply("213 20240102030405")
		case "REST":
			rest, _ = strconv.ParseInt(arg, 10, 64)
			reply("350 offset")
		case "EPSV":
			if passive != nil {
				_ = passive.Close()
			}
			passive, err = net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return
			}
			reply("229 passive (|||%d|)", passive.Addr().(*net.TCPAddr).Port)
		case "STOR", "RETR", "MLSD":
			if passive == nil {
				reply("425 passive required")
				continue
			}
			reply("150 data")
			data, err := passive.Accept()
			_ = passive.Close()
			passive = nil
			if err != nil {
				return
			}
			if tlsConfig != nil {
				conn := tls.Server(data, tlsConfig)
				if conn.Handshake() != nil {
					_ = data.Close()
					return
				}
				data = conn
			}
			if op == "STOR" {
				b, e := io.ReadAll(data)
				if e != nil {
					_ = data.Close()
					return
				}
				s.Put(arg, b)
			}
			if op == "RETR" {
				b, _ := s.Get(arg)
				if rest < int64(len(b)) {
					_, _ = data.Write(b[rest:])
				}
				rest = 0
			}
			if op == "MLSD" {
				s.Mu.Lock()
				for name, b := range s.Files {
					if path.Dir(name) == arg {
						_, _ = fmt.Fprintf(data, "type=file;size=%d;modify=20240102030405; %s\r\n", len(b), path.Base(name))
					}
				}
				s.Mu.Unlock()
			}
			_ = data.Close()
			reply("226 complete")
		case "RNFR":
			rename = arg
			reply("350 source")
		case "RNTO":
			s.before()
			if s.fault("publish_before") {
				return
			}
			s.Mu.Lock()
			b, ok := s.Files[rename]
			if ok {
				s.Files[arg] = b
				delete(s.Files, rename)
			}
			s.Mu.Unlock()
			if s.fault("publish_lost") {
				return
			}
			if ok {
				reply("250 moved")
			} else {
				reply("550 missing")
			}
		case "DELE":
			if s.fault("cleanup_error") {
				reply("450 busy")
			} else {
				s.Remove(arg)
				reply("250 removed")
			}
		case "QUIT":
			reply("221 bye")
			return
		default:
			reply("502 unsupported")
		}
	}
}
