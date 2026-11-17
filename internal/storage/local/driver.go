package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// Driver exposes local files only below one explicitly configured root.
type Driver struct {
	root string
}

func NewDriver(root string) (*Driver, error) {
	if root == "" {
		return nil, errors.New("local: root directory is required")
	}
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("local: root directory must be absolute: %q", root)
	}
	clean := filepath.Clean(root)
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return nil, fmt.Errorf("local: resolve root: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("local: stat root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("local: root is not a directory: %q", root)
	}
	return &Driver{root: filepath.Clean(resolved)}, nil
}

func (d *Driver) Scheme() string { return "file" }

func (d *Driver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(
		vfs.RandomRead,
		vfs.ParallelRandomRead,
		vfs.StreamingWrite,
		vfs.ServerSideList,
		vfs.AtomicPublishWrite,
		vfs.NoReplacePublish,
		vfs.ConditionalWrite,
	)
}

func (d *Driver) Probe(context.Context) (vfs.ProbeResult, error) {
	caps := d.StaticCapabilities()
	return vfs.ProbeResult{Supported: caps, Endpoint: d.root, SecurityProfile: "local-root"}, nil
}

// URIFromPath converts an absolute platform path to a canonical file URI.
func URIFromPath(localPath string) (vfs.URI, error) {
	if localPath == "" || !filepath.IsAbs(localPath) {
		return vfs.URI{}, fmt.Errorf("local: file path must be absolute: %q", localPath)
	}
	clean := filepath.Clean(localPath)
	slashed := filepath.ToSlash(clean)
	// url.URL.Path must start with slash for a Windows drive so the result is file:///C:/...
	if vol := filepath.VolumeName(clean); vol != "" && !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}
	raw := (&url.URL{Scheme: "file", Path: slashed}).String()
	return vfs.ParseURI(raw)
}

func infoIdentity(info os.FileInfo) vfs.ObjectIdentity {
	modified := info.ModTime().UTC()
	// Local files have no protocol ETag/version. Promote the existing durable
	// size+mtime identity into an opaque token so atomic publish/recovery can
	// persist and reconcile the same identity after rename and restart.
	etag := "local-" + strconv.FormatInt(info.Size(), 16) + "-" + strconv.FormatInt(modified.UnixNano(), 16)
	return vfs.ObjectIdentity{ETag: etag, Size: info.Size(), ModifiedAt: modified}
}

func (d *Driver) Stat(ctx context.Context, u vfs.URI) (vfs.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return vfs.ObjectInfo{}, err
	}
	p, err := d.localPath(u, true)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	info, err := os.Stat(p)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	if !info.Mode().IsRegular() {
		return vfs.ObjectInfo{}, fmt.Errorf("local: not a regular file: %s", u.String())
	}
	return vfs.ObjectInfo{URI: u, Size: info.Size(), Identity: infoIdentity(info)}, nil
}

func (d *Driver) List(ctx context.Context, u vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	if strings.ToLower(u.Scheme) != "file" {
		return vfs.ListPage{}, vfs.ErrUnsupportedScheme
	}
	if u.Host != "" && !strings.EqualFold(u.Host, "localhost") {
		return vfs.ListPage{}, fmt.Errorf("local: file URI host is not local: %q", u.Host)
	}
	prefix := strings.TrimPrefix(filepath.ToSlash(opts.Prefix), "/")
	entries := make([]vfs.ListEntry, 0)
	err := filepath.WalkDir(d.root, func(p string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		entryURI, err := URIFromPath(p)
		if err != nil {
			return err
		}
		if prefix != "" && !strings.HasPrefix(entryURI.ObjectKey(), prefix) {
			return nil
		}
		entries = append(entries, vfs.ListEntry{URI: entryURI, Size: info.Size(), Identity: infoIdentity(info)})
		return nil
	})
	if err != nil {
		return vfs.ListPage{}, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].URI.Path < entries[j].URI.Path })
	start := 0
	if opts.ContinuationToken != "" {
		n, err := strconv.Atoi(opts.ContinuationToken)
		if err != nil || n < 0 || n > len(entries) {
			return vfs.ListPage{}, errors.New("local: invalid continuation token")
		}
		start = n
	}
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	end := start + maxKeys
	if end > len(entries) {
		end = len(entries)
	}
	page := vfs.ListPage{Entries: append([]vfs.ListEntry(nil), entries[start:end]...)}
	if end < len(entries) {
		page.Truncated = true
		page.NextContinuationToken = strconv.Itoa(end)
	}
	return page, nil
}

// ReadRange 独立读取一个精确范围；连续扫描可通过 OpenRangeReader 复用已验证的句柄。
func (d *Driver) ReadRange(ctx context.Context, request vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := request.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if scope == nil || bm == nil {
		return vfs.RangeResult{}, errors.New("local: managed memory required")
	}
	if request.Length > int64(int(^uint(0)>>1)) {
		return vfs.RangeResult{}, fmt.Errorf("local: range length too large: %d", request.Length)
	}
	p, err := d.localPath(request.URI, true)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return vfs.RangeResult{}, err
	}
	f, err := os.Open(p)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	defer f.Close()
	return readFileRange(ctx, request, scope, bm, f, p, nil)
}

// readFileRange 保留读前、读后及路径身份检查，避免句柄复用掩盖文件修改或替换。
func readFileRange(ctx context.Context, request vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager, f *os.File, path string, snapshot *vfs.ObjectIdentity) (vfs.RangeResult, error) {
	if err := request.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if scope == nil || bm == nil {
		return vfs.RangeResult{}, errors.New("local: managed memory required")
	}
	if request.Length > int64(int(^uint(0)>>1)) {
		return vfs.RangeResult{}, fmt.Errorf("local: range length too large: %d", request.Length)
	}
	if err := ctx.Err(); err != nil {
		return vfs.RangeResult{}, err
	}
	before, err := f.Stat()
	if err != nil {
		return vfs.RangeResult{}, err
	}
	if !before.Mode().IsRegular() {
		return vfs.RangeResult{}, fmt.Errorf("local: not a regular file: %s", request.URI.String())
	}
	identity := infoIdentity(before)
	if snapshot != nil && !snapshot.Matches(identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	if request.Expected != nil && !request.Expected.Matches(identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	tag := request.Tag
	if tag == "" {
		tag = memory.TagNetworkBuffer
	}
	buf, err := bm.Allocate(scope, tag, int(request.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	fail := func(err error) (vfs.RangeResult, error) {
		buf.Release()
		return vfs.RangeResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	n, readErr := f.ReadAt(buf.Bytes(), request.Offset)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fail(readErr)
	}
	if int64(n) != request.Length {
		return fail(vfs.ErrShortRead)
	}
	afterFD, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if !identity.Matches(infoIdentity(afterFD)) {
		return fail(vfs.ErrObjectChanged)
	}
	// Also verify the pathname still names the same detectable version. On Unix an
	// open descriptor otherwise continues reading a replaced inode without mixing bytes.
	afterPath, err := os.Stat(path)
	if err != nil {
		return fail(vfs.ErrObjectChanged)
	}
	if !identity.Matches(infoIdentity(afterPath)) {
		return fail(vfs.ErrObjectChanged)
	}
	return vfs.RangeResult{Buffer: buf, Identity: identity}, nil
}

func (d *Driver) localPath(u vfs.URI, mustExist bool) (string, error) {
	if strings.ToLower(u.Scheme) != "file" {
		return "", vfs.ErrUnsupportedScheme
	}
	if u.Host != "" && !strings.EqualFold(u.Host, "localhost") {
		return "", fmt.Errorf("local: file URI host is not local: %q", u.Host)
	}
	p := filepath.FromSlash(u.Path)
	if vol := filepath.VolumeName(p); vol == "" && len(p) >= 3 && p[0] == filepath.Separator && p[2] == ':' {
		p = p[1:]
	}
	if !filepath.IsAbs(p) {
		return "", fmt.Errorf("local: file URI path must be absolute: %s", u.String())
	}
	p = filepath.Clean(p)
	if !withinRoot(d.root, p) {
		return "", fmt.Errorf("local: path escapes configured root: %s", u.String())
	}
	resolved, err := filepath.EvalSymlinks(p)
	if err == nil {
		if !withinRoot(d.root, resolved) {
			return "", fmt.Errorf("local: symlink escapes configured root: %s", u.String())
		}
		return p, nil
	}
	if !errors.Is(err, os.ErrNotExist) || mustExist {
		return "", err
	}
	parent, parentErr := filepath.EvalSymlinks(filepath.Dir(p))
	if parentErr != nil {
		return "", parentErr
	}
	if !withinRoot(d.root, parent) {
		return "", fmt.Errorf("local: parent escapes configured root: %s", u.String())
	}
	return p, nil
}

func withinRoot(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// keep time imported in this file's identity contract explicit across platforms.
var _ = time.Time{}
