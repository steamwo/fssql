package gosql

import (
	"context"
	"io/fs"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/steamwo/fssql/internal/storage/local"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// LocalFileConfig explicitly enables access to local files below RootDir.
// RootDir must be absolute; zero configuration keeps file:// disabled.
type LocalFileConfig struct {
	RootDir string
}

// FileURI converts an absolute platform path to a canonical, escaped file URI.
// Access is still subject to the LocalFiles.RootDir configured on Open.
func FileURI(path string) (string, error) {
	u, err := local.URIFromPath(path)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func publicLocalDriver(config LocalFileConfig, readOnly bool) (vfs.Driver, error) {
	driver, err := local.NewDriver(config.RootDir)
	if err != nil {
		return nil, err
	}
	if readOnly {
		manifest, err := local.URIFromPath(filepath.Join(config.RootDir, directoryCatalogFile))
		if err != nil {
			return nil, err
		}
		return readOnlyLocalDriver{Driver: driver, RangeReaderOpener: driver, walker: driver, manifestPath: manifest.Path}, nil
	}
	return driver, nil
}

// Expose only the local driver's reading interfaces and capabilities.
type readOnlyLocalDriver struct {
	vfs.Driver
	vfs.RangeReaderOpener
	walker       vfs.ListWalker
	manifestPath string
}

func (d readOnlyLocalDriver) isManifest(u vfs.URI) bool {
	return u.Path == d.manifestPath || (runtime.GOOS == "windows" && strings.EqualFold(u.Path, d.manifestPath))
}

func (d readOnlyLocalDriver) Stat(ctx context.Context, u vfs.URI) (vfs.ObjectInfo, error) {
	if d.isManifest(u) {
		return vfs.ObjectInfo{}, fs.ErrNotExist
	}
	return d.Driver.Stat(ctx, u)
}

func (d readOnlyLocalDriver) List(ctx context.Context, u vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	page, err := d.Driver.List(ctx, u, opts)
	page.Entries = slices.DeleteFunc(page.Entries, func(entry vfs.ListEntry) bool { return d.isManifest(entry.URI) })
	return page, err
}

func (d readOnlyLocalDriver) WalkList(ctx context.Context, u vfs.URI, prefix string, visit func(vfs.ListEntry) error) error {
	return d.walker.WalkList(ctx, u, prefix, func(entry vfs.ListEntry) error {
		if d.isManifest(entry.URI) {
			return nil
		}
		return visit(entry)
	})
}

func (d readOnlyLocalDriver) StaticCapabilities() vfs.CapabilitySet {
	return d.Driver.StaticCapabilities().Intersect(vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.ServerSideList))
}

func (d readOnlyLocalDriver) Probe(ctx context.Context) (vfs.ProbeResult, error) {
	result, err := d.Driver.Probe(ctx)
	result.Supported = result.Supported.Intersect(d.StaticCapabilities())
	return result, err
}
