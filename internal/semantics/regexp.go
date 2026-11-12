package semantics

import (
	"fmt"
	"regexp"
)

func CompileRegex(pattern, options string, full bool) (*regexp.Regexp, error) {
	insensitive, dot, newline, literal := false, false, false, false
	for _, c := range options {
		switch c {
		case 'i':
			insensitive = true
		case 'c':
			insensitive = false
		case 'l':
			literal = true
		case 's':
			dot = true
		case 'm', 'n', 'p':
			dot = false
		case 'g':
		case 0:
		default:
			return nil, fmt.Errorf("invalid regular expression option %q", c)
		}
	}
	if literal {
		pattern = regexp.QuoteMeta(pattern)
	}
	flags := ""
	if insensitive {
		flags += "i"
	}
	if dot {
		flags += "s"
	}
	if newline {
		flags += "m"
	}
	if flags != "" {
		pattern = "(?" + flags + ")" + pattern
	}
	if full {
		pattern = `\A(?:` + pattern + `)\z`
	}
	r, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regular expression: %w", err)
	}
	return r, nil
}
