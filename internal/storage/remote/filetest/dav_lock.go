package filetest

import (
	"fmt"
	"net/http"
	"strings"
)

func (s *Store) DropDAVLocks() { s.Mu.Lock(); defer s.Mu.Unlock(); s.davLocks = nil }

func (s *Store) serveDAVLock(w http.ResponseWriter, r *http.Request) bool {
	s.Mu.Lock()
	token := s.davLocks[r.URL.Path]
	_, exists := s.Files[r.URL.Path]
	s.Mu.Unlock()
	lockXML := func() string {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		return fmt.Sprintf(`<D:activelock><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype><D:depth>0</D:depth><D:timeout>Infinite</D:timeout><D:locktoken><D:href>%s</D:href></D:locktoken><D:lockroot><D:href>%s://%s%s</D:href></D:lockroot></D:activelock>`, token, scheme, r.Host, r.URL.EscapedPath())
	}
	switch r.Method {
	case "LOCK":
		if r.Header.Get("If-None-Match") != "*" || exists {
			w.WriteHeader(412)
			return true
		}
		if r.Header.Get("Depth") != "0" || r.Header.Get("Timeout") != "Infinite" {
			w.WriteHeader(400)
			return true
		}
		s.Mu.Lock()
		s.davSequence++
		token = "urn:uuid:" + strings.Trim(etag([]byte(fmt.Sprintf("%s/%d", r.URL.Path, s.davSequence))), `"`)
		if s.davLocks == nil {
			s.davLocks = map[string]string{}
		}
		s.davLocks[r.URL.Path] = token
		s.Files[r.URL.Path] = nil
		s.Mu.Unlock()
		if s.fault("create_lost") {
			lost(w)
			return true
		}
		w.Header().Set("Lock-Token", "<"+token+">")
		_, _ = fmt.Fprintf(w, `<D:prop xmlns:D="DAV:"><D:lockdiscovery>%s</D:lockdiscovery></D:prop>`, lockXML())
		return true
	case "PROPFIND":
		if !exists {
			w.WriteHeader(404)
			return true
		}
		locks := ""
		if token != "" {
			locks = lockXML()
		}
		w.WriteHeader(207)
		_, _ = fmt.Fprintf(w, `<D:multistatus xmlns:D="DAV:"><D:response><D:propstat><D:prop><D:lockdiscovery>%s</D:lockdiscovery></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>`, locks)
		return true
	}
	if exists && (token != "" || r.Header.Get("If") != "") {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		want := "<" + scheme + "://" + r.Host + r.URL.EscapedPath() + "> (<" + token + ">)"
		if token == "" || r.Header.Get("If") != want {
			w.WriteHeader(412)
			return true
		}
	}
	return false
}
