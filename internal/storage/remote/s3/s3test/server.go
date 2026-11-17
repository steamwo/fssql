// Package s3test provides an in-process S3 fixture for driver and public API tests.
package s3test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Object struct {
	Data                    []byte
	ETag, Version, Encoding string
}
type Part struct {
	Data           []byte
	ETag, Checksum string
}
type Upload struct {
	Path  string
	Parts map[int]Part
}
type Event struct{ Operation, Path, UploadID, IfNoneMatch, IfMatch, Version, AcceptEncoding string }
type Server struct {
	HTTP     *httptest.Server
	mu       sync.Mutex
	objects  map[string]Object
	uploads  map[string]Upload
	events   []Event
	fault    string
	sequence int
	pageSize int
}

func New() *Server {
	s := &Server{objects: map[string]Object{}, uploads: map[string]Upload{}}
	s.HTTP = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}
func (s *Server) Close()            { s.HTTP.Close() }
func (s *Server) URL() string       { return s.HTTP.URL }
func (s *Server) SetFault(f string) { s.mu.Lock(); defer s.mu.Unlock(); s.fault = f }
func (s *Server) SetPageSize(n int) { s.mu.Lock(); defer s.mu.Unlock(); s.pageSize = n }
func (s *Server) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}
func (s *Server) Counts(op string) int {
	n := 0
	for _, e := range s.Events() {
		if e.Operation == op {
			n++
		}
	}
	return n
}
func (s *Server) UploadCount() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.uploads) }
func (s *Server) Put(path string, data []byte, encoding string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.put(path, data, encoding)
}
func (s *Server) put(path string, data []byte, encoding string) {
	s.sequence++
	sum := sha256.Sum256(data)
	s.objects[path] = Object{Data: append([]byte{}, data...), ETag: hex.EncodeToString(sum[:]), Version: fmt.Sprint(s.sequence), Encoding: encoding}
}
func (s *Server) Object(path string) (Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[path]
	o.Data = append([]byte{}, o.Data...)
	return o, ok
}
func (s *Server) DeleteObject(path string) { s.mu.Lock(); defer s.mu.Unlock(); delete(s.objects, path) }
func (s *Server) MutatePart(number int, field string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, u := range s.uploads {
		p := u.Parts[number]
		switch field {
		case "missing":
			delete(u.Parts, number)
		case "etag":
			p.ETag = "changed"
			u.Parts[number] = p
		case "size":
			p.Data = append(p.Data, 'x')
			u.Parts[number] = p
		case "checksum":
			p.Checksum = "changed"
			u.Parts[number] = p
		case "upload":
			delete(s.uploads, id)
		}
		break
	}
}

func drop(w http.ResponseWriter) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := r.URL.Query()
	id := q.Get("uploadId")
	op := ""
	switch {
	case r.Method == "POST" && q.Has("uploads"):
		op = "create"
	case r.Method == "PUT" && id != "":
		op = "part"
	case r.Method == "POST" && id != "":
		op = "complete"
	case r.Method == "GET" && id != "":
		op = "list"
	case r.Method == "DELETE" && id != "":
		op = "abort"
	case r.Method == "PUT":
		op = "put"
	case r.Method == "HEAD":
		op = "head"
	case r.Method == "GET":
		op = "get"
	default:
		http.Error(w, "unexpected method", 400)
		return
	}
	s.events = append(s.events, Event{Operation: op, Path: r.URL.Path, UploadID: id, IfNoneMatch: r.Header.Get("If-None-Match"), IfMatch: r.Header.Get("If-Match"), Version: q.Get("versionId"), AcceptEncoding: r.Header.Get("Accept-Encoding")})
	if s.fault == op+"_before" {
		drop(w)
		return
	}
	if s.fault == op+"_forbidden" {
		w.WriteHeader(403)
		return
	}
	switch op {
	case "create":
		s.sequence++
		id = fmt.Sprintf("upload-%d", s.sequence)
		s.uploads[id] = Upload{Path: r.URL.Path, Parts: map[int]Part{}}
		if s.fault == "create_lost" {
			drop(w)
			return
		}
		fmt.Fprintf(w, "<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>", id)
	case "part":
		u, ok := s.uploads[id]
		if !ok || u.Path != r.URL.Path {
			w.WriteHeader(404)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		n, _ := strconv.Atoi(q.Get("partNumber"))
		sum := sha256.Sum256(data)
		p := Part{Data: data, ETag: hex.EncodeToString(sum[:]), Checksum: base64.StdEncoding.EncodeToString(sum[:])}
		u.Parts[n] = p
		w.Header().Set("ETag", `"`+p.ETag+`"`)
		w.Header().Set("x-amz-checksum-sha256", p.Checksum)
	case "list":
		u, ok := s.uploads[id]
		if !ok || u.Path != r.URL.Path {
			w.WriteHeader(404)
			return
		}
		if s.fault == "list_xml" {
			_, _ = io.WriteString(w, "<broken>")
			return
		}
		marker, _ := strconv.Atoi(q.Get("part-number-marker"))
		keys := make([]int, 0, len(u.Parts))
		for n := range u.Parts {
			if n > marker {
				keys = append(keys, n)
			}
		}
		sort.Ints(keys)
		max := 100
		if s.pageSize > 0 {
			max = s.pageSize
		}
		truncated := len(keys) > max
		if truncated {
			keys = keys[:max]
		}
		pieces := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		key := pieces[1]
		if s.fault == "list_foreign" {
			key = "other-job"
		}
		fmt.Fprintf(w, "<ListPartsResult><Bucket>%s</Bucket><Key>", pieces[0])
		_ = xml.EscapeText(w, []byte(key))
		fmt.Fprintf(w, "</Key><UploadId>%s</UploadId><IsTruncated>%t</IsTruncated>", id, truncated)
		if truncated {
			fmt.Fprintf(w, "<NextPartNumberMarker>%d</NextPartNumberMarker>", keys[len(keys)-1])
		}
		for _, n := range keys {
			p := u.Parts[n]
			fmt.Fprintf(w, "<Part><PartNumber>%d</PartNumber><ETag>&quot;%s&quot;</ETag><Size>%d</Size><ChecksumSHA256>%s</ChecksumSHA256></Part>", n, p.ETag, len(p.Data), p.Checksum)
		}
		_, _ = io.WriteString(w, "</ListPartsResult>")
	case "complete", "put":
		_, exists := s.objects[r.URL.Path]
		if s.fault == "complete_race" && op == "complete" {
			s.put(r.URL.Path, []byte("racing writer"), "")
			w.WriteHeader(412)
			return
		}
		if exists && r.Header.Get("If-None-Match") == "*" {
			w.WriteHeader(412)
			return
		}
		if s.fault == "complete_error" && op == "complete" {
			_, _ = io.WriteString(w, "<Error><Code>InvalidPart</Code></Error>")
			return
		}
		if s.fault == "complete_http_error" && op == "complete" {
			w.WriteHeader(400)
			return
		}
		var data []byte
		if op == "put" {
			data, _ = io.ReadAll(r.Body)
		} else {
			u, ok := s.uploads[id]
			if !ok || u.Path != r.URL.Path {
				w.WriteHeader(404)
				return
			}
			var request struct {
				Parts []struct {
					Number int    `xml:"PartNumber"`
					ETag   string `xml:"ETag"`
				} `xml:"Part"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
				w.WriteHeader(400)
				return
			}
			for _, part := range request.Parts {
				p, ok := u.Parts[part.Number]
				if !ok || strings.Trim(part.ETag, `"`) != p.ETag {
					w.WriteHeader(400)
					return
				}
				data = append(data, p.Data...)
			}
			delete(s.uploads, id)
		}
		s.put(r.URL.Path, data, "")
		obj := s.objects[r.URL.Path]
		w.Header().Set("ETag", `"`+obj.ETag+`"`)
		w.Header().Set("x-amz-version-id", obj.Version)
		if s.fault == op+"_lost" {
			drop(w)
			return
		}
		if op == "complete" {
			if s.fault == "complete_xml" {
				_, _ = io.WriteString(w, "<CompleteMultipartUploadResult>")
				return
			}
			fmt.Fprintf(w, "<CompleteMultipartUploadResult><ETag>&quot;%s&quot;</ETag></CompleteMultipartUploadResult>", obj.ETag)
			if s.fault == "complete_trailing" {
				_, _ = io.WriteString(w, "<Error><Code>InternalError</Code></Error>")
			}
		}
	case "abort":
		if s.fault == "abort_error" {
			w.WriteHeader(500)
			return
		}
		if u, ok := s.uploads[id]; !ok || u.Path != r.URL.Path {
			w.WriteHeader(404)
			return
		}
		delete(s.uploads, id)
		w.WriteHeader(204)
	case "head", "get":
		obj, ok := s.objects[r.URL.Path]
		if !ok {
			w.WriteHeader(404)
			return
		}
		if r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != `"`+obj.ETag+`"` {
			w.WriteHeader(412)
			return
		}
		if q.Get("versionId") != "" && q.Get("versionId") != obj.Version {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("ETag", `"`+obj.ETag+`"`)
		w.Header().Set("x-amz-version-id", obj.Version)
		if obj.Encoding != "" {
			w.Header().Set("Content-Encoding", obj.Encoding)
		}
		if op == "head" {
			w.Header().Set("Content-Length", strconv.Itoa(len(obj.Data)))
			return
		}
		if s.fault == "get_lost" {
			drop(w)
			return
		}
		if raw := r.Header.Get("Range"); raw != "" {
			var start, end int
			if _, err := fmt.Sscanf(raw, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= len(obj.Data) {
				w.WriteHeader(416)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.Data)))
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.WriteHeader(206)
			_, _ = w.Write(obj.Data[start : end+1])
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.Data)))
		_, _ = w.Write(obj.Data)
	}
}
