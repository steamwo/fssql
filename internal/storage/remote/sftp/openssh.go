package sftp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/steamwo/fssql/internal/storage/remote"
)

type OpenSSHConfig struct {
	Binary         string
	User           string
	KnownHostsFile string
	IdentityFile   string
	ConnectTimeout time.Duration
}

func (c OpenSSHConfig) Validate() error {
	if c.User == "" {
		return errors.New("sftp: OpenSSH user is required")
	}
	if c.KnownHostsFile == "" {
		return errors.New("sftp: known_hosts file is required; insecure host-key mode is unsupported")
	}
	return nil
}

type processTransport struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	cmd    *exec.Cmd
	stderr *bytes.Buffer
	once   sync.Once
}

func (p *processTransport) Read(b []byte) (int, error)  { return p.stdout.Read(b) }
func (p *processTransport) Write(b []byte) (int, error) { return p.stdin.Write(b) }
func (p *processTransport) Close() error {
	var first error
	p.once.Do(func() {
		_ = p.stdin.Close()
		_ = p.stdout.Close()
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		_ = p.cmd.Wait()
	})
	return first
}

type OpenSSHFactory struct{ Config OpenSSHConfig }

func (f OpenSSHFactory) buildArgs(key remote.ClientKey) (string, []string, error) {
	if err := f.Config.Validate(); err != nil {
		return "", nil, err
	}
	endpoint := key.Endpoint
	if endpoint == "" {
		return "", nil, errors.New("sftp: empty endpoint")
	}
	host := endpoint
	port := ""
	if h, p, err := net.SplitHostPort(endpoint); err == nil {
		host = h
		port = p
	}
	binary := f.Config.Binary
	if binary == "" {
		binary = "ssh"
	}
	timeout := f.Config.ConnectTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	seconds := int(timeout / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	args := []string{"-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "CheckHostIP=yes", "-o", "UserKnownHostsFile=" + f.Config.KnownHostsFile, "-o", "ConnectTimeout=" + strconv.Itoa(seconds)}
	if port != "" {
		args = append(args, "-p", port)
	}
	if f.Config.IdentityFile != "" {
		args = append(args, "-i", f.Config.IdentityFile)
	}
	args = append(args, "-s", f.Config.User+"@"+host, "sftp")
	return binary, args, nil
}
func (f OpenSSHFactory) Dial(ctx context.Context, key remote.ClientKey) (Session, error) {
	binary, args, err := f.buildArgs(key)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr := new(bytes.Buffer)
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, classifySSHError(stderr.String(), err)
	}
	tr := &processTransport{stdin: stdin, stdout: stdout, cmd: cmd, stderr: stderr}
	session, err := NewWireSession(tr)
	if err != nil {
		_ = tr.Close()
		return nil, classifySSHError(stderr.String(), err)
	}
	return session, nil
}
func classifySSHError(stderr string, err error) error {
	lower := strings.ToLower(stderr)
	if strings.Contains(lower, "host key verification failed") || strings.Contains(lower, "remote host identification has changed") || strings.Contains(lower, "no ") && strings.Contains(lower, "host key is known") || strings.Contains(lower, "host key mismatch") {
		return fmt.Errorf("%w: %v", ErrHostKey, err)
	}
	return fmt.Errorf("%w: %v", ErrConnection, err)
}
