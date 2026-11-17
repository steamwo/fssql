package fileset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

var (
	ErrFileLimit      = errors.New("fileset: matched file limit exceeded")
	ErrInvalidListing = errors.New("fileset: invalid paginated listing")
	ErrNoMatches      = errors.New("fileset: no files matched")
)

const defaultMaxFiles = 100000

type PlanOptions struct {
	View             vfs.ObjectViewSpec
	HivePartitioning bool
	MaxFiles         int
	ListPageSize     int
}

type FileSnapshot struct {
	URI            vfs.URI
	Size           int64
	Identity       vfs.ObjectIdentity
	SourceIdentity vfs.ObjectIdentity
	Partitions     map[string]string
}

func (f FileSnapshot) clone() FileSnapshot {
	out := f
	if f.Partitions != nil {
		out.Partitions = make(map[string]string, len(f.Partitions))
		for k, v := range f.Partitions {
			out.Partitions[k] = v
		}
	}
	return out
}

type ScanSet struct {
	files       []FileSnapshot
	fingerprint string
	pattern     string
	prefix      string
}

func newScanSet(pattern, prefix string, files []FileSnapshot) *ScanSet {
	copyFiles := make([]FileSnapshot, len(files))
	for i := range files {
		copyFiles[i] = files[i].clone()
	}
	return &ScanSet{
		files:       copyFiles,
		fingerprint: datasetFingerprint(copyFiles),
		pattern:     pattern,
		prefix:      prefix,
	}
}

func (s *ScanSet) Files() []FileSnapshot {
	out := make([]FileSnapshot, len(s.files))
	for i := range s.files {
		out[i] = s.files[i].clone()
	}
	return out
}

func (s *ScanSet) MatchedFiles() int     { return len(s.files) }
func (s *ScanSet) Fingerprint() string   { return s.fingerprint }
func (s *ScanSet) Pattern() string       { return s.pattern }
func (s *ScanSet) ListingPrefix() string { return s.prefix }

type Planner struct {
	registry *vfs.Registry
}

func NewPlanner(registry *vfs.Registry) *Planner { return &Planner{registry: registry} }

func (p *Planner) Plan(ctx context.Context, rawPattern string, opts PlanOptions) (*ScanSet, error) {
	if p == nil || p.registry == nil {
		return nil, errors.New("fileset: nil VFS registry")
	}
	patternURI, err := vfs.ParseURI(rawPattern)
	if err != nil {
		return nil, err
	}
	if patternURI.IsHTTP() && patternURI.HasGlob() {
		return nil, errors.New("fileset: HTTP URL glob is unsupported")
	}
	driver, err := p.registry.DriverFor(patternURI)
	if err != nil {
		return nil, err
	}
	maxFiles := opts.MaxFiles
	if maxFiles <= 0 {
		maxFiles = defaultMaxFiles
	}
	if !patternURI.HasGlob() {
		obj, err := p.registry.ResolveObject(ctx, patternURI, opts.View)
		if err != nil {
			return nil, err
		}
		f := snapshotFromInfo(vfs.ObjectInfo{URI: obj.URI, Size: obj.Size, Identity: obj.Identity}, opts.HivePartitioning)
		f.SourceIdentity = obj.SourceIdentity
		return newScanSet(patternURI.String(), patternURI.ObjectKey(), []FileSnapshot{f}), nil
	}

	prefix := extractPrefix(patternURI.ObjectKey())
	base, err := vfs.ParseURI(patternURI.Scheme + "://" + patternURI.Host + "/")
	if err != nil {
		return nil, err
	}
	pageSize := opts.ListPageSize
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	files := make([]FileSnapshot, 0, min(maxFiles, pageSize))
	continuation := ""
	seenTokens := map[string]struct{}{}
	for {
		page, err := driver.List(ctx, base, vfs.ListOptions{Prefix: prefix, ContinuationToken: continuation, MaxKeys: pageSize})
		if err != nil {
			return nil, err
		}
		for _, entry := range page.Entries {
			matched, err := matchPath(patternURI.ObjectKey(), entry.URI.ObjectKey())
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			if len(files) >= maxFiles {
				return nil, fmt.Errorf("%w: max=%d", ErrFileLimit, maxFiles)
			}
			f := snapshotFromEntry(entry, opts.HivePartitioning)
			if p.registry.HasObjectViews() {
				obj, err := p.registry.ResolveObject(ctx, entry.URI, opts.View)
				if err != nil {
					return nil, err
				}
				f.URI, f.Size, f.Identity, f.SourceIdentity = obj.URI, obj.Size, obj.Identity, obj.SourceIdentity
			}
			files = append(files, f)
		}
		if !page.Truncated {
			break
		}
		if page.NextContinuationToken == "" {
			return nil, fmt.Errorf("%w: truncated page missing continuation token", ErrInvalidListing)
		}
		if _, exists := seenTokens[page.NextContinuationToken]; exists {
			return nil, fmt.Errorf("%w: repeated continuation token %q", ErrInvalidListing, page.NextContinuationToken)
		}
		seenTokens[page.NextContinuationToken] = struct{}{}
		continuation = page.NextContinuationToken
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoMatches, patternURI.String())
	}
	// Existing protocol listings keep their path order; no display serialization
	// participates in resource identity or comparison.
	sort.Slice(files, func(i, j int) bool { return files[i].URI.Path < files[j].URI.Path })
	return newScanSet(patternURI.String(), prefix, files), nil
}

func hasGlob(s string) bool { return strings.ContainsAny(s, "*?[") }

func extractPrefix(pattern string) string {
	for i, r := range pattern {
		if r == '*' || r == '?' || r == '[' {
			return pattern[:i]
		}
	}
	return pattern
}

func matchPath(pattern, key string) (bool, error) {
	p := strings.Split(pattern, "/")
	k := strings.Split(key, "/")
	return matchSegments(p, k)
}

func matchSegments(pattern, key []string) (bool, error) {
	if len(pattern) == 0 {
		return len(key) == 0, nil
	}
	if pattern[0] == "**" {
		if len(pattern) == 1 {
			return true, nil
		}
		for i := 0; i <= len(key); i++ {
			ok, err := matchSegments(pattern[1:], key[i:])
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	if len(key) == 0 {
		return false, nil
	}
	ok, err := path.Match(pattern[0], key[0])
	if err != nil || !ok {
		return false, err
	}
	return matchSegments(pattern[1:], key[1:])
}

func snapshotFromInfo(info vfs.ObjectInfo, hive bool) FileSnapshot {
	f := FileSnapshot{URI: info.URI, Size: info.Size, Identity: info.Identity}
	if hive {
		f.Partitions = discoverHivePartitions(info.URI.ObjectKey())
	}
	return f
}

func snapshotFromEntry(entry vfs.ListEntry, hive bool) FileSnapshot {
	f := FileSnapshot{URI: entry.URI, Size: entry.Size, Identity: entry.Identity}
	if hive {
		f.Partitions = discoverHivePartitions(entry.URI.ObjectKey())
	}
	return f
}

func discoverHivePartitions(key string) map[string]string {
	parts := strings.Split(key, "/")
	out := make(map[string]string)
	for _, seg := range parts[:max(0, len(parts)-1)] {
		name, value, ok := strings.Cut(seg, "=")
		if !ok || name == "" {
			continue
		}
		out[name] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func datasetFingerprint(files []FileSnapshot) string {
	h := sha256.New()
	ordered := make([]FileSnapshot, len(files))
	copy(ordered, files)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].URI.IdentityKey()+ordered[i].URI.ViewKey < ordered[j].URI.IdentityKey()+ordered[j].URI.ViewKey
	})
	for _, f := range ordered {
		_, _ = h.Write([]byte(f.URI.IdentityKey()))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(f.URI.ViewKey))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(f.Identity.SnapshotID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(f.Identity.ModifiedAt.UTC().Format("2006-01-02T15:04:05.999999999Z")))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(fmt.Sprintf("%q/%q/%d/%s", f.SourceIdentity.VersionID, f.SourceIdentity.ETag, f.SourceIdentity.Size, f.SourceIdentity.ModifiedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"))))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(f.Identity.VersionID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(f.Identity.ETag))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatInt(f.Size, 10)))
		_, _ = h.Write([]byte{0})
		keys := make([]string, 0, len(f.Partitions))
		for k := range f.Partitions {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			_, _ = h.Write([]byte(k))
			_, _ = h.Write([]byte{'='})
			_, _ = h.Write([]byte(f.Partitions[k]))
			_, _ = h.Write([]byte{0})
		}
		_, _ = h.Write([]byte{1})
	}
	return hex.EncodeToString(h.Sum(nil))
}
