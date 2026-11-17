// Package filetest provides deterministic, in-process protocol fixtures for
// file publication and public COPY/recovery tests. It is not a real-service test.
package filetest

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

type Store struct {
	Mu            sync.Mutex
	Files         map[string][]byte
	Events        []string
	Fault         string
	Hardlink      string
	BeforePublish func()
	DAVLocks      bool
	DAVWeakETag   bool
	davLocks      map[string]string
	davSequence   uint64
}

func New() *Store                  { return &Store{Files: make(map[string][]byte), Hardlink: "1"} }
func (s *Store) SetFault(f string) { s.Mu.Lock(); defer s.Mu.Unlock(); s.Fault = f }
func (s *Store) Put(p string, b []byte) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Files[p] = append([]byte(nil), b...)
}
func (s *Store) Get(p string) ([]byte, bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	b, ok := s.Files[p]
	return append([]byte(nil), b...), ok
}
func (s *Store) Remove(p string) { s.Mu.Lock(); defer s.Mu.Unlock(); delete(s.Files, p) }
func (s *Store) Count(op string) int {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	n := 0
	for _, event := range s.Events {
		if event == op {
			n++
		}
	}
	return n
}
func (s *Store) StagingCount() int {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	n := 0
	for p := range s.Files {
		if strings.Contains(p, ".fssql-") {
			n++
		}
	}
	return n
}
func (s *Store) event(op string)     { s.Mu.Lock(); defer s.Mu.Unlock(); s.Events = append(s.Events, op) }
func (s *Store) fault(f string) bool { s.Mu.Lock(); defer s.Mu.Unlock(); return s.Fault == f }
func (s *Store) before() {
	s.Mu.Lock()
	f := s.BeforePublish
	s.Mu.Unlock()
	if f != nil {
		f()
	}
}
func etag(b []byte) string { h := sha256.Sum256(b); return fmt.Sprintf(`"%x"`, h[:]) }
func lost(w http.ResponseWriter) {
	if h, ok := w.(http.Hijacker); ok {
		c, _, e := h.Hijack()
		if e == nil {
			_ = c.Close()
		}
	}
}

func (s *Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if user, pass, ok := r.BasicAuth(); !ok || user != "user" || pass != "pass" {
		w.WriteHeader(401)
		return
	}
	op := r.Method
	s.event(op)
	if op == "LOCK" && !s.DAVLocks {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.DAVLocks && s.serveDAVLock(w, r) {
		return
	}
	if op == http.MethodOptions {
		w.Header().Set("Allow", "OPTIONS, HEAD, GET, PUT, MOVE, DELETE")
		w.WriteHeader(204)
		return
	}
	b, exists := s.Get(r.URL.Path)
	if op == "MOVE" {
		s.before()
	}
	if (op == "GET" || op == "HEAD") && s.fault("read_error") && !strings.Contains(r.URL.Path, ".fssql-") {
		w.WriteHeader(503)
		return
	}
	if op == "PUT" {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		s.Mu.Lock()
		current, ok := s.Files[r.URL.Path]
		if r.Header.Get("If-None-Match") == "*" && ok || r.Header.Get("If-Match") != "" && (!ok || r.Header.Get("If-Match") != etag(current)) {
			s.Mu.Unlock()
			w.WriteHeader(412)
			return
		}
		s.Files[r.URL.Path] = append([]byte(nil), data...)
		s.Mu.Unlock()
		if s.fault("create_lost") && r.Header.Get("If-None-Match") == "*" {
			lost(w)
			return
		}
		w.Header().Set("ETag", etag(data))
		w.WriteHeader(201)
		return
	}
	if !exists {
		w.WriteHeader(404)
		return
	}
	if match := r.Header.Get("If-Match"); match != "" && match != etag(b) {
		w.WriteHeader(412)
		return
	}
	switch op {
	case "HEAD", "GET":
		tag := etag(b)
		if s.DAVWeakETag {
			tag = "W/" + tag
		}
		w.Header().Set("ETag", tag)
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		if s.fault("content_encoding") {
			w.Header().Set("Content-Encoding", "gzip")
		}
		if op == "HEAD" {
			return
		}
		if rg := r.Header.Get("Range"); rg != "" {
			var a, z int
			if _, e := fmt.Sscanf(rg, "bytes=%d-%d", &a, &z); e != nil || a < 0 || z >= len(b) || z < a {
				w.WriteHeader(416)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, z, len(b)))
			w.Header().Set("Content-Length", strconv.Itoa(z-a+1))
			w.WriteHeader(206)
			_, _ = w.Write(b[a : z+1])
			return
		}
		_, _ = w.Write(b)
	case "MOVE":
		dest, err := url.Parse(r.Header.Get("Destination"))
		if err != nil {
			w.WriteHeader(400)
			return
		}
		if r.Header.Get("Overwrite") != "F" {
			w.WriteHeader(400)
			return
		}
		if s.fault("publish_before") {
			lost(w)
			return
		}
		s.Mu.Lock()
		_, ok := s.Files[dest.Path]
		if ok {
			s.Mu.Unlock()
			w.WriteHeader(412)
			return
		}
		s.Files[dest.Path] = b
		delete(s.Files, r.URL.Path)
		delete(s.davLocks, r.URL.Path)
		s.Mu.Unlock()
		if s.fault("publish_lost") {
			lost(w)
			return
		}
		w.WriteHeader(201)
	case "DELETE":
		if s.fault("cleanup_error") {
			w.WriteHeader(503)
			return
		}
		s.Remove(r.URL.Path)
		s.Mu.Lock()
		delete(s.davLocks, r.URL.Path)
		s.Mu.Unlock()
		w.WriteHeader(204)
	default:
		w.WriteHeader(405)
	}
}

// Listen serves one protocol per accepted connection; Close also stops active
// sessions so tests can assert resource release without orphan goroutines.
func Listen(handler func(net.Conn)) (net.Listener, func(), error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	var mu sync.Mutex
	connections := map[net.Conn]bool{}
	var wg sync.WaitGroup
	closed := false
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				_ = c.Close()
				return
			}
			connections[c] = true
			wg.Add(1)
			mu.Unlock()
			go func() {
				defer wg.Done()
				defer func() { mu.Lock(); delete(connections, c); mu.Unlock(); _ = c.Close() }()
				handler(c)
			}()
		}
	}()
	closeFn := func() {
		_ = l.Close()
		mu.Lock()
		closed = true
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	}
	return l, closeFn, nil
}
