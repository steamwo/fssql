package local

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

type productionReviewSwapContext struct {
	context.Context
	once sync.Once
	swap func()
}

func (c *productionReviewSwapContext) Err() error { c.once.Do(c.swap); return nil }

func TestProductionReviewSymlinkSwapEscapesRoot(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	inside := filepath.Join(root, "inside")
	if err := os.Mkdir(inside, 0700); err != nil {
		t.Fatal(err)
	}
	insidePath := filepath.Join(inside, "data.txt")
	outsidePath := filepath.Join(outside, "data.txt")
	if err := os.WriteFile(insidePath, []byte("INSIDE"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outsidePath, []byte("SECRET"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "input")
	makeLink := func(target string) error {
		if runtime.GOOS == "windows" {
			return exec.Command("cmd.exe", "/c", "mklink", "/J", link, target).Run()
		}
		return os.Symlink(target, link)
	}
	if err := makeLink(inside); err != nil {
		t.Skipf("directory link unavailable: %v", err)
	}
	driver, err := NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	scope, buffers := managed(t, 32)
	defer scope.Close()
	var swapErr error
	ctx := &productionReviewSwapContext{Context: context.Background(), swap: func() {
		if swapErr = os.Remove(link); swapErr == nil {
			swapErr = makeLink(outside)
		}
	}}
	result, err := driver.ReadRange(ctx, vfs.RangeRequest{URI: mustURI(t, filepath.Join(link, "data.txt")), Offset: 0, Length: 6}, scope, buffers)
	if swapErr != nil {
		t.Fatal(swapErr)
	}
	if err != nil {
		return
	}
	defer result.Buffer.Release()
	if got := string(result.Buffer.Bytes()); got != "INSIDE" {
		t.Fatalf("read redirected after symlink replacement: %q", got)
	}
}
