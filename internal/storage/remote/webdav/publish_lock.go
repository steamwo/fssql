package webdav

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

const publishLockBody = `<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
const discoverLockBody = `<D:propfind xmlns:D="DAV:"><D:prop><D:lockdiscovery/></D:prop></D:propfind>`

// Only an infinite exclusive depth-zero lock can protect an unbounded streaming
// PUT across process death. Finite leases are rejected rather than reacquired:
// a new lease cannot prove ownership of the resource left by the old lease.
// The manifest holds a digest; lockdiscovery supplies the token again after
// restart, using the currently authorized endpoint credentials.
type publishLock struct {
	driver       *Driver
	stage        vfs.URI
	proof, token string
}

type activePublishLock struct {
	Exclusive *struct{} `xml:"DAV: lockscope>exclusive"`
	Write     *struct{} `xml:"DAV: locktype>write"`
	Depth     string    `xml:"DAV: depth"`
	Timeout   string    `xml:"DAV: timeout"`
	Token     string    `xml:"DAV: locktoken>href"`
	Root      string    `xml:"DAV: lockroot>href"`
}

func lockDigest(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

func (g *publishLock) restore(stage vfs.URI, proof string) error {
	if proof != "etag" && (len(proof) != 64 || strings.Trim(proof, "0123456789abcdef") != "") {
		return vfs.ErrInvalidSealedObject
	}
	g.stage, g.proof = stage, proof
	return nil
}

func (g *publishLock) valid(l activePublishLock) bool {
	if l.Exclusive == nil || l.Write == nil || l.Depth != "0" || l.Timeout != "Infinite" || len(l.Token) == 0 || len(l.Token) > 4096 || strings.ContainsAny(l.Token, "<> \t\r\n\x00") {
		return false
	}
	token, err := url.Parse(l.Token)
	if err != nil || !token.IsAbs() {
		return false
	}
	// mod_dav_fs omits lockroot. The conditional LOCK response is bound to
	// our request URI; later discovery must match its globally unique token.
	if l.Root == "" {
		return true
	}
	root, err := url.Parse(l.Root)
	target, targetErr := g.driver.targetURL(g.stage)
	return err == nil && targetErr == nil && root.Path == target.Path && (!root.IsAbs() || (root.Scheme == target.Scheme && root.Host == target.Host))
}

func (g *publishLock) create(ctx context.Context, stage vfs.URI) (bool, error) {
	g.stage = stage
	r, err := g.driver.newRequest(ctx, "LOCK", stage, strings.NewReader(publishLockBody))
	if err != nil {
		return false, err
	}
	r.Header.Set("Content-Type", "application/xml; charset=utf-8")
	r.Header.Set("Depth", "0")
	r.Header.Set("Timeout", "Infinite")
	r.Header.Set("If-None-Match", "*")
	resp, err := g.driver.doPublish(r)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	// Only an explicit unsupported method permits the strong-ETag protocol.
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return false, statusError("LOCK exclusive staging", resp.StatusCode)
	}
	b, err := readBounded(resp.Body, 64<<10)
	if err != nil {
		return false, err
	}
	var document struct {
		Locks []activePublishLock `xml:"DAV: lockdiscovery>activelock"`
	}
	if xml.Unmarshal(b, &document) != nil || len(document.Locks) != 1 || !g.valid(document.Locks[0]) || resp.Header.Get("Lock-Token") != "<"+document.Locks[0].Token+">" {
		return false, fmt.Errorf("%w: WebDAV staging requires an infinite exclusive depth-zero lock", vfs.ErrInvalidSealedObject)
	}
	g.token = document.Locks[0].Token
	g.proof = lockDigest(g.token)
	return true, nil
}

func (g *publishLock) condition(ctx context.Context, u vfs.URI) (string, error) {
	if g.proof == "" || g.proof == "etag" || u.IdentityKey() != g.stage.IdentityKey() {
		return "", nil
	}
	if g.token == "" {
		r, err := g.driver.newRequest(ctx, "PROPFIND", u, strings.NewReader(discoverLockBody))
		if err != nil {
			return "", err
		}
		r.Header.Set("Depth", "0")
		r.Header.Set("Content-Type", "application/xml; charset=utf-8")
		resp, err := g.driver.doPublish(r)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusMultiStatus {
			return "", statusError("PROPFIND ownership", resp.StatusCode)
		}
		b, err := readBounded(resp.Body, 64<<10)
		if err != nil {
			return "", err
		}
		var document struct {
			Responses []struct {
				Props []struct {
					Status string              `xml:"DAV: status"`
					Locks  []activePublishLock `xml:"DAV: prop>lockdiscovery>activelock"`
				} `xml:"DAV: propstat"`
			} `xml:"DAV: response"`
		}
		if xml.Unmarshal(b, &document) != nil {
			return "", vfs.ErrInvalidSealedObject
		}
		for _, response := range document.Responses {
			for _, prop := range response.Props {
				parts := strings.Fields(prop.Status)
				if len(parts) < 2 || parts[1] != "200" {
					continue
				}
				for _, l := range prop.Locks {
					if g.valid(l) && lockDigest(l.Token) == g.proof {
						g.token = l.Token
					}
				}
			}
		}
		if g.token == "" {
			return "", vfs.ErrInvalidSealedObject
		}
	}
	target, err := g.driver.targetURL(u)
	if err != nil {
		return "", err
	}
	// Tagged conditions also cover Apache's validation of the source parent
	// during MOVE; an untagged condition is evaluated against that parent too.
	return "<" + target.String() + "> (<" + g.token + ">)", nil
}
