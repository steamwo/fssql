package expression

import (
	"fmt"
	"github.com/steamwo/fssql/internal/semantics"
	"regexp"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
)

func compileSQLRegex(pattern, options string, full bool) (*regexp.Regexp, error) {
	return semantics.CompileRegex(pattern, options, full)
}

func regexArguments(id function.ID, args []ScalarValue) (string, int, error) {
	options := ""
	group := 0
	if id == function.RegexpReplaceID {
		if len(args) == 4 {
			options = string(args[3].Bytes)
		}
	} else if id == function.RegexpExtractID || id == function.RegexpExtractAllID {
		if len(args) >= 3 {
			if args[2].Type.ID == types.Varchar {
				options = string(args[2].Bytes)
			} else {
				if args[2].Int64 < 0 || args[2].Int64 > 9 {
					return "", 0, fmt.Errorf("regexp_extract group must be between 0 and 9")
				}
				group = int(args[2].Int64)
			}
		}
		if len(args) == 4 {
			options = string(args[3].Bytes)
		}
	} else if len(args) == 3 {
		options = string(args[2].Bytes)
	}
	if id != function.RegexpReplaceID && strings.ContainsRune(options, 'g') {
		return "", 0, fmt.Errorf("global option only applies to regexp_replace")
	}
	return options, group, nil
}

func regexScalar(id function.ID, result types.LogicalType, args []ScalarValue) (ScalarValue, error) {
	out := ScalarValue{Type: result, Valid: true}
	s := string(args[0].Bytes)
	if id == function.RegexpEscapeID {
		var b strings.Builder
		for _, c := range []byte(s) {
			if c == 0 {
				b.WriteString(`\x00`)
				continue
			}
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c >= 128) {
				b.WriteByte('\\')
			}
			b.WriteByte(c)
		}
		out.Bytes = []byte(b.String())
		return out, nil
	}
	options, group, err := regexArguments(id, args)
	if err != nil {
		return out, err
	}
	r, err := compileSQLRegex(string(args[1].Bytes), options, id == function.RegexpFullMatchID)
	if err != nil {
		return out, err
	}
	switch id {
	case function.RegexpMatchesID, function.RegexpFullMatchID:
		out.Bool = r.MatchString(s)
	case function.RegexpExtractID:
		if group > r.NumSubexp() {
			return out, fmt.Errorf("regexp_extract group exceeds capture count")
		}
		m := r.FindStringSubmatch(s)
		if len(m) > group {
			out.Bytes = []byte(m[group])
		} else {
			out.Bytes = []byte{}
		}
	case function.RegexpReplaceID:
		replacement := string(args[2].Bytes)
		for i := 0; i < len(replacement); i++ {
			if replacement[i] != '\\' {
				continue
			}
			i++
			if i >= len(replacement) || (replacement[i] != '\\' && (replacement[i] < '0' || replacement[i] > '9')) {
				return out, fmt.Errorf("invalid regexp replacement escape")
			}
			if replacement[i] >= '0' && replacement[i] <= '9' && int(replacement[i]-'0') > r.NumSubexp() {
				return out, fmt.Errorf("regexp replacement refers to an absent capture group")
			}
		}
		limit := 1
		if strings.ContainsRune(options, 'g') {
			limit = -1
		}
		matches := r.FindAllStringSubmatchIndex(s, limit)
		var b strings.Builder
		last := 0
		for _, m := range matches {
			b.WriteString(s[last:m[0]])
			for i := 0; i < len(replacement); i++ {
				if replacement[i] != '\\' {
					b.WriteByte(replacement[i])
					continue
				}
				i++
				if i >= len(replacement) {
					return out, fmt.Errorf("invalid regexp replacement escape")
				}
				c := replacement[i]
				if c == '\\' {
					b.WriteByte(c)
				} else if c >= '0' && c <= '9' {
					n := int(c-'0') * 2
					if n+1 < len(m) && m[n] >= 0 {
						b.WriteString(s[m[n]:m[n+1]])
					}
				} else {
					return out, fmt.Errorf("invalid regexp replacement escape")
				}
			}
			last = m[1]
		}
		b.WriteString(s[last:])
		out.Bytes = []byte(b.String())
	}
	return out, nil
}
