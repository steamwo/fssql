package remote_test

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/remote/ftp"
	"github.com/steamwo/fssql/internal/storage/remote/s3"
	"github.com/steamwo/fssql/internal/storage/remote/sftp"
	"github.com/steamwo/fssql/internal/storage/remote/webdav"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// TestProtocolStaticCapabilityMatrix 固定各远端协议的静态能力上界，防止驱动未来误报未实现能力。
func TestProtocolStaticCapabilityMatrix(t *testing.T) {
	s3Driver, err := s3.NewDriver(s3.Config{
		Endpoint: "https://s3.example.com", Region: "us-east-1",
		CredentialScope: "secret:s3", CredentialVersion: "1", SecurityProfile: "strict",
	}, s3.Credentials{AccessKey: "access", SecretKey: "secret"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ftpKey := remote.ClientKey{Endpoint: "ftp.example.com:21", CredentialScope: "secret:ftp", CredentialVersion: "1", SecurityProfile: "plain-opt-in"}
	ftpDriver, err := ftp.NewDriver("ftp", ftpKey, ftp.NewManager(2, 2, nil), 1)
	if err != nil {
		t.Fatal(err)
	}

	sftpKey := remote.ClientKey{Endpoint: "sftp.example.com:22", CredentialScope: "secret:sftp", CredentialVersion: "1", SecurityProfile: "strict"}
	sftpDriver, err := sftp.NewDriver(sftpKey, sftp.NewManager(2, 2, nil), 1)
	if err != nil {
		t.Fatal(err)
	}

	webdavKey := remote.ClientKey{Endpoint: "dav.example.com:80", CredentialScope: "secret:dav", CredentialVersion: "1", SecurityProfile: "strict"}
	webdavDriver, err := webdav.NewDriver(
		"webdav", webdavKey, remote.NewClientManager(2, nil),
		webdav.CredentialProviderFunc(func(context.Context, remote.ClientKey, string) (webdav.Credentials, error) {
			return webdav.Credentials{Username: "user", Password: "pass"}, nil
		}),
		security.NewNetworkPolicy(80), 2,
	)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		driver vfs.Driver
		want   vfs.CapabilitySet
	}{
		{
			name: "s3", driver: s3Driver,
			want: vfs.Capabilities(
				vfs.RandomRead, vfs.ParallelRandomRead, vfs.StreamingWrite, vfs.MultipartWrite,
				vfs.AtomicPublishWrite, vfs.NoReplacePublish, vfs.ConditionalRead, vfs.ConditionalWrite, vfs.StableETag, vfs.VersionedObject,
				vfs.ServerSideList, vfs.IncompleteWriteRecovery,
			),
		},
		{
			name: "sftp", driver: sftpDriver,
			want: vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.StreamingRead, vfs.StreamingWrite, vfs.ServerSideList, vfs.NoReplacePublish),
		},
		{
			name: "ftp", driver: ftpDriver,
			want: vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.StreamingRead, vfs.StreamingWrite, vfs.ServerSideList),
		},
		{
			name: "webdav", driver: webdavDriver,
			want: vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.StreamingRead, vfs.StreamingWrite, vfs.ServerSideList),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.driver.StaticCapabilities()
			if got != tc.want {
				t.Fatalf("static capabilities=%#x, want=%#x", uint64(got), uint64(tc.want))
			}
			if got.Has(vfs.AtomicRename) {
				t.Fatalf("%s must not advertise AtomicRename", tc.name)
			}
		})
	}
}
