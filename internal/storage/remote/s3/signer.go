package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

type Credentials struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
}

func (c Credentials) Validate() error {
	if c.AccessKey == "" || c.SecretKey == "" {
		return fmt.Errorf("s3: missing access key or secret key")
	}
	return nil
}

type SigV4Signer struct {
	Region string
	Creds  Credentials
}

func (s SigV4Signer) Sign(req *http.Request, now time.Time, payloadHash string) error {
	if err := s.Creds.Validate(); err != nil {
		return err
	}
	if s.Region == "" {
		return fmt.Errorf("s3: empty region")
	}
	if payloadHash == "" {
		payloadHash = emptyPayloadSHA256
	}
	amzDate := now.UTC().Format("20060102T150405Z")
	date := now.UTC().Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if s.Creds.SessionToken != "" {
		req.Header.Set("x-amz-security-token", s.Creds.SessionToken)
	}
	canonicalHeaders, signedHeaders := canonicalHeaderBlock(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQuery(req.URL.Query()),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := date + "/" + s.Region + "/s3/aws4_request"
	crHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(crHash[:])
	kDate := hmacSHA256([]byte("AWS4"+s.Creds.SecretKey), date)
	kRegion := hmacSHA256(kDate, s.Region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.Creds.AccessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+sig)
	return nil
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

func canonicalURI(u *url.URL) string {
	if u.EscapedPath() == "" {
		return "/"
	}
	return u.EscapedPath()
}

func canonicalQuery(v url.Values) string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := append([]string(nil), v[k]...)
		sort.Strings(vals)
		if len(vals) == 0 {
			vals = []string{""}
		}
		for _, val := range vals {
			parts = append(parts, awsEncode(k)+"="+awsEncode(val))
		}
	}
	return strings.Join(parts, "&")
}

func awsEncode(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func canonicalHeaderBlock(req *http.Request) (string, string) {
	names := []string{"host"}
	for name := range req.Header {
		name = strings.ToLower(name)
		if name == "authorization" || name == "user-agent" || name == "content-length" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	uniq := names[:0]
	for _, n := range names {
		if len(uniq) == 0 || uniq[len(uniq)-1] != n {
			uniq = append(uniq, n)
		}
	}
	var b strings.Builder
	for _, name := range uniq {
		var value string
		if name == "host" {
			value = req.Host
			if value == "" {
				value = req.URL.Host
			}
		} else {
			values := req.Header.Values(name)
			for i := range values {
				values[i] = strings.Join(strings.Fields(values[i]), " ")
			}
			value = strings.Join(values, ",")
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strings.TrimSpace(value))
		b.WriteByte('\n')
	}
	return b.String(), strings.Join(uniq, ";")
}
