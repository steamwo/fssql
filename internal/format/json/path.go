package json

import (
	"fmt"
	"strings"
)

type ProjectedPathSet struct{ paths [][]string }

func NewProjectedPathSet(paths ...string) (ProjectedPathSet, error) {
	out := ProjectedPathSet{paths: make([][]string, 0, len(paths))}
	for _, raw := range paths {
		if raw == "" {
			return ProjectedPathSet{}, fmt.Errorf("json: empty projected path")
		}
		parts := strings.Split(raw, ".")
		for _, p := range parts {
			if p == "" {
				return ProjectedPathSet{}, fmt.Errorf("json: invalid projected path %q", raw)
			}
		}
		out.paths = append(out.paths, parts)
	}
	return out, nil
}
func (p ProjectedPathSet) Empty() bool { return len(p.paths) == 0 }
func (p ProjectedPathSet) Relevant(path []string) bool {
	if len(p.paths) == 0 {
		return true
	}
	for _, q := range p.paths {
		if prefix(q, path) || prefix(path, q) {
			return true
		}
	}
	return false
}
func prefix(a, b []string) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
