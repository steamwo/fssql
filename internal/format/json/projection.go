package json

import "strings"

type pathNode struct {
	terminal bool
	children map[string]*pathNode
}

type ProjectedPathSet struct {
	root *pathNode
	all  bool
}

func NewProjectedPathSet(paths ...string) *ProjectedPathSet {
	p := &ProjectedPathSet{root: &pathNode{children: map[string]*pathNode{}}}
	if len(paths) == 0 {
		p.all = true
		return p
	}
	for _, raw := range paths {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		n := p.root
		for _, seg := range strings.Split(raw, ".") {
			if n.children[seg] == nil {
				n.children[seg] = &pathNode{children: map[string]*pathNode{}}
			}
			n = n.children[seg]
		}
		n.terminal = true
	}
	return p
}

func (p *ProjectedPathSet) Includes(path ...string) bool {
	if p == nil || p.all {
		return true
	}
	n := p.root
	for _, seg := range path {
		n = n.children[seg]
		if n == nil {
			return false
		}
	}
	return n.terminal || len(n.children) > 0
}

func (p *ProjectedPathSet) Exact(path ...string) bool {
	if p == nil || p.all {
		return true
	}
	n := p.root
	for _, seg := range path {
		n = n.children[seg]
		if n == nil {
			return false
		}
	}
	return n.terminal
}
