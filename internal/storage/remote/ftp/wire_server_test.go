package ftp

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// protocolServer 是只实现本驱动所需命令的确定性 FTP/FTPS 测试服务器。
type protocolServer struct {
	listener  net.Listener
	tlsConfig *tls.Config
	secure    bool
	mu        sync.Mutex
	files     map[string][]byte
	closed    chan struct{}
}

func newProtocolServer(t *testing.T, secure bool) (*protocolServer, *x509.CertPool) {
	t.Helper()
	server := &protocolServer{secure: secure, files: map[string][]byte{"/data.bin": []byte("abcdefghij")}, closed: make(chan struct{})}
	var roots *x509.CertPool
	if secure {
		cert, pool := testCertificate(t)
		server.tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		roots = pool
	}
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if secure {
		server.listener = tls.NewListener(base, server.tlsConfig)
	} else {
		server.listener = base
	}
	go server.serve()
	t.Cleanup(func() { _ = server.Close() })
	return server, roots
}

func (s *protocolServer) Addr() string { return s.listener.Addr().String() }

func (s *protocolServer) Close() error {
	select {
	case <-s.closed:
		return nil
	default:
		close(s.closed)
		return s.listener.Close()
	}
}

func (s *protocolServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *protocolServer) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	writeReply := func(format string, args ...any) bool {
		if _, err := fmt.Fprintf(writer, format, args...); err != nil {
			return false
		}
		return writer.Flush() == nil
	}
	if !writeReply("220 test ftp ready\r\n") {
		return
	}
	var passive net.Listener
	var rest int64
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		command, argument, _ := strings.Cut(line, " ")
		command = strings.ToUpper(command)
		switch command {
		case "USER":
			if !writeReply("331 password required\r\n") {
				return
			}
		case "PASS":
			if !writeReply("230 logged in\r\n") {
				return
			}
		case "TYPE", "PBSZ", "PROT":
			if !writeReply("200 ok\r\n") {
				return
			}
		case "FEAT":
			if !writeReply("211-Features\r\n REST STREAM\r\n MLST type*;size*;modify*;\r\n211 End\r\n") {
				return
			}
		case "SIZE":
			s.mu.Lock()
			data, ok := s.files[argument]
			s.mu.Unlock()
			if !ok {
				if !writeReply("550 not found\r\n") {
					return
				}
				continue
			}
			if !writeReply("213 %d\r\n", len(data)) {
				return
			}
		case "MDTM":
			if argument == "/slow" {
				// 不响应，用于验证 context cancellation 能中断 control read。
				continue
			}
			if !writeReply("213 20240102030405\r\n") {
				return
			}
		case "EPSV":
			if passive != nil {
				_ = passive.Close()
			}
			base, listenErr := net.Listen("tcp", "127.0.0.1:0")
			if listenErr != nil {
				_ = writeReply("425 cannot open data\r\n")
				continue
			}
			if s.secure {
				passive = tls.NewListener(base, s.tlsConfig)
			} else {
				passive = base
			}
			port := passive.Addr().(*net.TCPAddr).Port
			if !writeReply("229 Entering Extended Passive Mode (|||%d|)\r\n", port) {
				return
			}
		case "REST":
			value, parseErr := strconv.ParseInt(argument, 10, 64)
			if parseErr != nil || value < 0 {
				if !writeReply("501 invalid offset\r\n") {
					return
				}
				continue
			}
			rest = value
			if !writeReply("350 restart accepted\r\n") {
				return
			}
		case "RETR":
			if passive == nil {
				if !writeReply("425 no passive listener\r\n") {
					return
				}
				continue
			}
			if !writeReply("150 opening data\r\n") {
				return
			}
			dataConn, acceptErr := passive.Accept()
			_ = passive.Close()
			passive = nil
			if acceptErr != nil {
				return
			}
			s.mu.Lock()
			data := append([]byte(nil), s.files[argument]...)
			s.mu.Unlock()
			if rest < int64(len(data)) {
				_, _ = dataConn.Write(data[rest:])
			}
			rest = 0
			_ = dataConn.Close()
			if !writeReply("226 transfer complete\r\n") {
				return
			}
		case "MLSD":
			if passive == nil {
				if !writeReply("425 no passive listener\r\n") {
					return
				}
				continue
			}
			if !writeReply("150 opening data\r\n") {
				return
			}
			dataConn, acceptErr := passive.Accept()
			_ = passive.Close()
			passive = nil
			if acceptErr != nil {
				return
			}
			s.mu.Lock()
			for path, data := range s.files {
				name := strings.TrimPrefix(path, "/")
				_, _ = fmt.Fprintf(dataConn, "type=file;size=%d;modify=20240102030405; %s\r\n", len(data), name)
			}
			s.mu.Unlock()
			_ = dataConn.Close()
			if !writeReply("226 transfer complete\r\n") {
				return
			}
		case "STOR":
			if passive == nil {
				if !writeReply("425 no passive listener\r\n") {
					return
				}
				continue
			}
			if !writeReply("150 opening data\r\n") {
				return
			}
			dataConn, acceptErr := passive.Accept()
			_ = passive.Close()
			passive = nil
			if acceptErr != nil {
				return
			}
			data, readErr := io.ReadAll(dataConn)
			_ = dataConn.Close()
			if readErr != nil {
				return
			}
			s.mu.Lock()
			s.files[argument] = data
			s.mu.Unlock()
			if !writeReply("226 transfer complete\r\n") {
				return
			}
		case "DELE":
			s.mu.Lock()
			delete(s.files, argument)
			s.mu.Unlock()
			if !writeReply("250 deleted\r\n") {
				return
			}
		case "QUIT":
			_ = writeReply("221 bye\r\n")
			return
		default:
			if !writeReply("502 unsupported\r\n") {
				return
			}
		}
	}
}

func testCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ftp test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return certificate, pool
}
