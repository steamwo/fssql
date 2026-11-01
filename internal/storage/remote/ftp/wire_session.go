package ftp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	// maxControlLine 限制单行 FTP control response，防止恶意 endpoint 导致无界 Go heap 增长。
	maxControlLine = 64 << 10
	// maxFeatureBytes 限制 FEAT 多行响应累计大小。
	maxFeatureBytes = 256 << 10
	// maxListEntries 限制一次 MLSD 返回的元数据项数；超限时显式失败而不是静默截断。
	maxListEntries = 4096
	// maxListBytes 限制一次 MLSD data stream 的文本元数据字节数。
	maxListBytes = 4 << 20
)

var (
	// ErrProtocol 表示 FTP control/data 协议响应不符合预期。
	ErrProtocol = errors.New("ftp: protocol error")
	// ErrListLimit 表示远端目录列表超过当前受控元数据上限。
	ErrListLimit = errors.New("ftp: list limit exceeded")
)

// wireSession 是单个 FTP control connection；Pool 保证它同一时间只被一个 lease 使用。
type wireSession struct {
	// scheme 决定 data channel 是否必须使用 TLS。
	scheme string
	// control/reader/writer 管理 control channel。
	control net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	// selectedIP 是通过 NetworkPolicy 校验并实际建立 control connection 的 IP；data channel 只能复用它。
	selectedIP net.IP
	// serverName 用于 FTPS data TLS 的 SNI/证书校验。
	serverName string
	// tlsConfig 是 endpoint-scoped TLS 配置快照。
	tlsConfig *tls.Config
	// dial 是 data connection 使用的拨号函数。
	dial DialContextFunc
	// mu 防止意外并发 control command 破坏 FTP 响应序列。
	mu sync.Mutex
	// closed 标记 control session 已终止。
	closed bool
}

// bootstrap 完成 greeting、认证、binary mode，以及 FTPS data channel protection。
func (s *wireSession) bootstrap(ctx context.Context, credentials Credentials) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	code, message, err := s.readResponseLocked(ctx)
	if err != nil {
		return err
	}
	if code != 220 {
		return unexpectedCode("greeting", code, message, 220)
	}
	if err := validateCommandArg(credentials.Username); err != nil {
		return fmt.Errorf("ftp: invalid username: %w", err)
	}
	if err := validateCommandArg(credentials.Password); err != nil {
		return fmt.Errorf("ftp: invalid password: %w", err)
	}
	code, message, err = s.commandLocked(ctx, "USER "+credentials.Username)
	if err != nil {
		return err
	}
	if code == 331 {
		code, message, err = s.commandLocked(ctx, "PASS "+credentials.Password)
		if err != nil {
			return err
		}
	}
	if code != 230 {
		return unexpectedCode("login", code, message, 230)
	}
	code, message, err = s.commandLocked(ctx, "TYPE I")
	if err != nil {
		return err
	}
	if code != 200 {
		return unexpectedCode("TYPE I", code, message, 200)
	}
	if s.scheme == "ftps" {
		code, message, err = s.commandLocked(ctx, "PBSZ 0")
		if err != nil {
			return err
		}
		if code != 200 {
			return unexpectedCode("PBSZ", code, message, 200)
		}
		code, message, err = s.commandLocked(ctx, "PROT P")
		if err != nil {
			return err
		}
		if code != 200 {
			return unexpectedCode("PROT", code, message, 200)
		}
	}
	return nil
}

// Features 读取 FEAT，并仅识别当前驱动可安全消费的 REST STREAM 与 MLSD。
func (s *wireSession) Features(ctx context.Context) (Features, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	code, message, err := s.commandLocked(ctx, "FEAT")
	if err != nil {
		return Features{}, err
	}
	if code == 500 || code == 502 {
		return Features{}, nil
	}
	if code != 211 {
		return Features{}, unexpectedCode("FEAT", code, message, 211)
	}
	upper := strings.ToUpper(message)
	return Features{RestStream: strings.Contains(upper, "REST STREAM"), MLSD: strings.Contains(upper, "MLST") || strings.Contains(upper, "MLSD")}, nil
}

// Stat 使用 SIZE + 可选 MDTM 获取文件身份；不依赖人类可读 LIST。
func (s *wireSession) Stat(ctx context.Context, path string) (FileInfo, error) {
	if err := validateCommandArg(path); err != nil {
		return FileInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	code, message, err := s.commandLocked(ctx, "SIZE "+path)
	if err != nil {
		return FileInfo{}, err
	}
	if code != 213 {
		return FileInfo{}, unexpectedCode("SIZE", code, message, 213)
	}
	sizeText := firstResponseText(message)
	size, err := strconv.ParseInt(strings.TrimSpace(sizeText), 10, 64)
	if err != nil || size < 0 {
		return FileInfo{}, fmt.Errorf("%w: invalid SIZE response %q", ErrProtocol, message)
	}
	info := FileInfo{Size: size}
	code, message, err = s.commandLocked(ctx, "MDTM "+path)
	if err != nil {
		return FileInfo{}, err
	}
	if code == 213 {
		stamp := strings.TrimSpace(firstResponseText(message))
		if len(stamp) >= 14 {
			if parsed, parseErr := time.Parse("20060102150405", stamp[:14]); parseErr == nil {
				info.ModUnix = parsed.UTC().Unix()
			}
		}
	}
	return info, nil
}

// List 使用 MLSD data stream，并对元数据条数/字节数设置显式上限。
func (s *wireSession) List(ctx context.Context, path string) ([]DirEntry, error) {
	if err := validateCommandArg(path); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.openTransferLocked(ctx, "MLSD "+path)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	reader := bufio.NewReaderSize(io.LimitReader(data, maxListBytes+1), 64<<10)
	entries := make([]DirEntry, 0, 64)
	var consumed int64
	for {
		line, err := reader.ReadString('\n')
		consumed += int64(len(line))
		if consumed > maxListBytes {
			_ = data.Close()
			_ = s.finishTransferLocked(ctx, true)
			return nil, ErrListLimit
		}
		line = strings.TrimSpace(line)
		if line != "" {
			entry, parseErr := parseMLSDLine(line)
			if parseErr != nil {
				_ = data.Close()
				_ = s.finishTransferLocked(ctx, true)
				return nil, parseErr
			}
			if entry.Name != "." && entry.Name != ".." {
				entries = append(entries, entry)
				if len(entries) > maxListEntries {
					_ = data.Close()
					_ = s.finishTransferLocked(ctx, true)
					return nil, ErrListLimit
				}
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = data.Close()
			_ = s.finishTransferLocked(ctx, true)
			return nil, err
		}
	}
	if err := data.Close(); err != nil {
		_ = s.finishTransferLocked(ctx, true)
		return nil, err
	}
	if err := s.finishTransferLocked(ctx, false); err != nil {
		return nil, err
	}
	return entries, nil
}

// ReadAt 使用 REST offset + RETR，只读取调用方请求的长度，然后显式结束 data transfer。
func (s *wireSession) ReadAt(ctx context.Context, path string, dst []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("ftp: negative read offset %d", offset)
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if err := validateCommandArg(path); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	code, message, err := s.commandLocked(ctx, "REST "+strconv.FormatInt(offset, 10))
	if err != nil {
		return 0, err
	}
	if code != 350 {
		return 0, fmt.Errorf("%w: REST unavailable: code=%d message=%q", vfs.ErrRangeUnsupported, code, message)
	}
	data, err := s.openTransferLocked(ctx, "RETR "+path)
	if err != nil {
		return 0, err
	}
	stop := armContext(ctx, data)
	n, readErr := io.ReadFull(data, dst)
	stop()
	_ = data.Close()
	if readErr != nil && ctx.Err() != nil {
		return n, ctx.Err()
	}
	finishErr := s.finishTransferLocked(ctx, n == len(dst))
	if readErr != nil {
		if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
			return n, io.EOF
		}
		return n, readErr
	}
	if finishErr != nil {
		return n, finishErr
	}
	return n, nil
}

// OpenWrite 打开 STOR data stream；完成确认由 wireStreamWriter.Close 读取。
func (s *wireSession) OpenWrite(ctx context.Context, path string) (StreamWriter, error) {
	if err := validateCommandArg(path); err != nil {
		return nil, err
	}
	s.mu.Lock()
	data, err := s.openTransferLocked(ctx, "STOR "+path)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	return &wireStreamWriter{session: s, data: data, sessionLocked: true}, nil
}

// Delete 删除远端文件。
func (s *wireSession) Delete(ctx context.Context, path string) error {
	if err := validateCommandArg(path); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	code, message, err := s.commandLocked(ctx, "DELE "+path)
	if err != nil {
		return err
	}
	if code != 250 {
		return unexpectedCode("DELE", code, message, 250)
	}
	return nil
}

// Close 最佳努力发送 QUIT，然后关闭 control connection。
func (s *wireSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.control == nil {
		s.closed = true
		return nil
	}
	_, _, _ = s.commandLocked(context.Background(), "QUIT")
	s.closed = true
	return s.control.Close()
}

// openTransferLocked 通过 EPSV 创建与 control 同一已校验 IP 的 data connection，然后启动传输命令。
// 调用方必须持有 s.mu。
func (s *wireSession) openTransferLocked(ctx context.Context, command string) (net.Conn, error) {
	code, message, err := s.commandLocked(ctx, "EPSV")
	if err != nil {
		return nil, err
	}
	if code != 229 {
		return nil, unexpectedCode("EPSV", code, message, 229)
	}
	port, err := parseEPSVPort(message)
	if err != nil {
		return nil, err
	}
	address := net.JoinHostPort(s.selectedIP.String(), strconv.Itoa(port))
	data, err := s.dial(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("%w: dial data connection: %v", ErrConnection, err)
	}
	code, message, err = s.commandLocked(ctx, command)
	if err != nil {
		_ = data.Close()
		return nil, err
	}
	if code != 125 && code != 150 {
		_ = data.Close()
		return nil, unexpectedCode(command, code, message, 125, 150)
	}
	if s.scheme == "ftps" {
		tlsConn := tls.Client(data, s.tlsConfig.Clone())
		if err := handshakeTLS(ctx, tlsConn); err != nil {
			_ = data.Close()
			return nil, fmt.Errorf("%w: ftps data handshake: %v", ErrConnection, err)
		}
		data = tlsConn
	}
	return data, nil
}

// finishTransferLocked 消费 transfer final response；earlyClose 允许服务器以 426 表示客户端主动结束 range transfer。
// 调用方必须持有 s.mu。
func (s *wireSession) finishTransferLocked(ctx context.Context, earlyClose bool) error {
	code, message, err := s.readResponseLocked(ctx)
	if err != nil {
		return err
	}
	if code == 226 || code == 250 {
		return nil
	}
	if earlyClose && code == 426 {
		code, message, err = s.readResponseLocked(ctx)
		if err != nil {
			return err
		}
		if code == 226 || code == 225 {
			return nil
		}
	}
	return unexpectedCode("transfer completion", code, message, 226)
}

// commandLocked 写入单条 control command 并读取完整 FTP response；调用方必须持有 s.mu。
func (s *wireSession) commandLocked(ctx context.Context, command string) (int, string, error) {
	if s.closed {
		return 0, "", io.ErrClosedPipe
	}
	if strings.ContainsAny(command, "\r\n\x00") {
		return 0, "", fmt.Errorf("%w: command contains control characters", ErrProtocol)
	}
	stop := armContext(ctx, s.control)
	defer stop()
	if _, err := s.writer.WriteString(command + "\r\n"); err != nil {
		if ctx.Err() != nil {
			return 0, "", ctx.Err()
		}
		return 0, "", fmt.Errorf("%w: control write: %v", ErrConnection, err)
	}
	if err := s.writer.Flush(); err != nil {
		if ctx.Err() != nil {
			return 0, "", ctx.Err()
		}
		return 0, "", fmt.Errorf("%w: control flush: %v", ErrConnection, err)
	}
	code, message, err := s.readResponseNoArmLocked()
	if err != nil && ctx.Err() != nil {
		return 0, "", ctx.Err()
	}
	return code, message, err
}

// readResponseLocked 读取一条 response，并让 context cancellation 通过 deadline 中断阻塞 I/O。
func (s *wireSession) readResponseLocked(ctx context.Context) (int, string, error) {
	stop := armContext(ctx, s.control)
	defer stop()
	code, message, err := s.readResponseNoArmLocked()
	if err != nil && ctx.Err() != nil {
		return 0, "", ctx.Err()
	}
	return code, message, err
}

// readResponseNoArmLocked 解析 RFC 959 单行/多行响应；调用方已负责 deadline。
func (s *wireSession) readResponseNoArmLocked() (int, string, error) {
	line, err := readControlLine(s.reader)
	if err != nil {
		return 0, "", fmt.Errorf("%w: control read: %v", ErrConnection, err)
	}
	if len(line) < 3 {
		return 0, "", fmt.Errorf("%w: short response %q", ErrProtocol, line)
	}
	code, err := strconv.Atoi(line[:3])
	if err != nil {
		return 0, "", fmt.Errorf("%w: invalid response code %q", ErrProtocol, line)
	}
	message := responseText(line)
	if len(line) >= 4 && line[3] == '-' {
		var builder strings.Builder
		builder.WriteString(message)
		prefix := line[:3] + " "
		for {
			next, readErr := readControlLine(s.reader)
			if readErr != nil {
				return 0, "", fmt.Errorf("%w: multiline response: %v", ErrConnection, readErr)
			}
			if builder.Len()+len(next)+1 > maxFeatureBytes {
				return 0, "", fmt.Errorf("%w: multiline response exceeds %d bytes", ErrProtocol, maxFeatureBytes)
			}
			if builder.Len() > 0 {
				builder.WriteByte('\n')
			}
			if strings.HasPrefix(next, prefix) {
				builder.WriteString(responseText(next))
				break
			}
			builder.WriteString(next)
		}
		message = builder.String()
	}
	return code, message, nil
}

// armContext 把 context cancellation/deadline 映射到 net.Conn deadline，避免为每次 I/O 创建无界 goroutine。
func armContext(ctx context.Context, conn net.Conn) func() {
	if conn == nil {
		return func() {}
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Time{})
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	return func() {
		stop()
		_ = conn.SetDeadline(time.Time{})
	}
}

// wireStreamWriter 持有 wireSession 的 control mutex，直到 Close/Abort 消费完 final response。
type wireStreamWriter struct {
	// session 拥有 control channel。
	session *wireSession
	// data 是当前 STOR data connection。
	data net.Conn
	// sessionLocked 表示 OpenWrite 尚未归还 session.mu。
	sessionLocked bool
	// terminal 防止重复结束 transfer。
	terminal bool
}

// Write 写入 data connection，并观察 context cancellation。
func (w *wireStreamWriter) Write(ctx context.Context, data []byte) (int, error) {
	if w == nil || w.terminal || w.data == nil {
		return 0, io.ErrClosedPipe
	}
	stop := armContext(ctx, w.data)
	defer stop()
	n, err := w.data.Write(data)
	if err != nil && ctx.Err() != nil {
		return n, ctx.Err()
	}
	return n, err
}

// Close 完成 data stream 并消费 226 final response，然后释放 control mutex。
func (w *wireStreamWriter) Close(ctx context.Context) error {
	if w == nil || w.terminal {
		return io.ErrClosedPipe
	}
	w.terminal = true
	closeErr := w.data.Close()
	finishErr := w.session.finishTransferLocked(ctx, false)
	w.unlockSession()
	if closeErr != nil {
		return closeErr
	}
	return finishErr
}

// Abort 主动关闭 data stream，消费 server abort/final response，并释放 control mutex。
func (w *wireStreamWriter) Abort(ctx context.Context) error {
	if w == nil || w.terminal {
		return nil
	}
	w.terminal = true
	_ = w.data.Close()
	err := w.session.finishTransferLocked(ctx, true)
	w.unlockSession()
	return err
}

// unlockSession 确保每个 STOR writer 只释放一次 control mutex。
func (w *wireStreamWriter) unlockSession() {
	if w.sessionLocked {
		w.sessionLocked = false
		w.session.mu.Unlock()
	}
}
