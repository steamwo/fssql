package binder

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/sql/ast"
)

// BoundCopyEncryption owns decoded raw key bytes until execution/query cleanup.
// No passphrase derivation, wrapping or callback resolution is performed.
type BoundCopyEncryption struct {
	KeyID     string
	Algorithm string
	key       []byte
}

func (*BoundCopyEncryption) String() string     { return "COPY encryption <redacted>" }
func (c *BoundCopyEncryption) GoString() string { return c.String() }
func (c *BoundCopyEncryption) Key() []byte      { return c.key }
func (c *BoundCopyEncryption) Clear() {
	if c == nil {
		return
	}
	clear(c.key)
	c.key = nil
}

func bindCopyEncryption(expr ast.Expr) (*BoundCopyEncryption, error) {
	config, ok := expr.(*ast.CopyEncryptionExpr)
	if !ok {
		return nil, fmt.Errorf("ENCRYPTION_CONFIG requires a struct")
	}
	out := &BoundCopyEncryption{}
	valid := false
	defer func() {
		if !valid {
			out.Clear()
		}
	}()
	seen := map[string]bool{}
	for _, field := range config.Fields {
		name := strings.ToLower(field.Name.Name)
		if seen[name] {
			return nil, fmt.Errorf("duplicate ENCRYPTION_CONFIG field")
		}
		seen[name] = true
		switch name {
		case "master_key_id":
			value, err := copyRequiredString(field.Value)
			if err != nil || strings.TrimSpace(value) == "" {
				return nil, fmt.Errorf("ENCRYPTION_CONFIG requires non-empty master_key_id string")
			}
			out.KeyID = strings.TrimSpace(value)
		case "footer_key_value":
			call, ok := field.Value.(*ast.CallExpr)
			if !ok || len(call.Function.Parts) != 1 || !strings.EqualFold(call.Function.Parts[0].Name, "from_base64") || len(call.Args) != 1 || call.Star || call.Distinct {
				return nil, fmt.Errorf("footer_key_value requires from_base64 with one string literal")
			}
			encoded, err := copyRequiredString(call.Args[0])
			if err != nil {
				return nil, fmt.Errorf("footer_key_value requires from_base64 with one string literal")
			}
			// AES keys require canonical padded standard Base64 (24 or 44 bytes).
			if (len(encoded) != 24 && len(encoded) != 44) || strings.ContainsAny(encoded, "\r\n") {
				return nil, fmt.Errorf("footer_key_value requires Base64 for a 16/32-byte raw AES key")
			}
			out.key, err = base64.StdEncoding.Strict().DecodeString(encoded)
			if err != nil {
				return nil, fmt.Errorf("invalid footer_key_value Base64")
			}
			switch len(out.key) {
			case 16:
				out.Algorithm = "AES-128-GCM"
			case 32:
				out.Algorithm = "AES-256-GCM"
			default:
				return nil, fmt.Errorf("footer_key_value requires a 16/32-byte raw AES key")
			}
		default:
			return nil, fmt.Errorf("unknown ENCRYPTION_CONFIG field")
		}
	}
	if !seen["master_key_id"] || !seen["footer_key_value"] {
		return nil, fmt.Errorf("ENCRYPTION_CONFIG requires footer_key_value and master_key_id")
	}
	valid = true
	return out, nil
}
