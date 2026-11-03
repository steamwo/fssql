package gosql

import (
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

func publicLocalDriver(config LocalFileConfig) (vfs.Driver, error) {
	return local.NewDriver(config.RootDir)
}
