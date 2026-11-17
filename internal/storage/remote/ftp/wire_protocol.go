package ftp

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// readControlLine 读取受上限保护的单行 response。
func readControlLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", fmt.Errorf("%w: control line exceeds %d bytes", ErrProtocol, maxControlLine)
	}
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(line), "\r\n"), nil
}

// responseText 去掉 FTP code 与分隔符。
func responseText(line string) string {
	if len(line) <= 4 {
		return ""
	}
	return line[4:]
}

// firstResponseText 返回多行 response 的第一段文本。
func firstResponseText(message string) string {
	if index := strings.IndexByte(message, '\n'); index >= 0 {
		return message[:index]
	}
	return message
}

// unexpectedCode 统一构造协议状态码错误。
func unexpectedCode(operation string, code int, message string, expected ...int) error {
	return &statusError{operation, code, message, expected}
}

type statusError struct {
	operation string
	code      int
	message   string
	expected  []int
}

func (e *statusError) Error() string {
	return fmt.Sprintf("%v: %s code=%d expected=%v message=%q", ErrProtocol, e.operation, e.code, e.expected, e.message)
}
func (e *statusError) Unwrap() error { return ErrProtocol }

// validateCommandArg 防止 URI/credential 中的 CRLF 注入 FTP control channel。
func validateCommandArg(value string) error {
	if strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("%w: control character in command argument", ErrProtocol)
	}
	return nil
}

// parseEPSVPort 解析 229 Extended Passive Mode response 中的 data port。
func parseEPSVPort(message string) (int, error) {
	start := strings.LastIndex(message, "(")
	end := strings.LastIndex(message, ")")
	if start < 0 || end <= start+4 {
		return 0, fmt.Errorf("%w: invalid EPSV response %q", ErrProtocol, message)
	}
	body := message[start+1 : end]
	delim := body[0]
	parts := strings.Split(body, string(delim))
	if len(parts) < 5 {
		return 0, fmt.Errorf("%w: invalid EPSV tuple %q", ErrProtocol, body)
	}
	port, err := strconv.Atoi(parts[3])
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("%w: invalid EPSV port %q", ErrProtocol, parts[3])
	}
	return port, nil
}

// parseMLSDLine 解析机器可读 facts；未知 fact 被忽略，type/size/modify 按需消费。
func parseMLSDLine(line string) (DirEntry, error) {
	separator := strings.IndexByte(line, ' ')
	if separator <= 0 || separator == len(line)-1 {
		return DirEntry{}, fmt.Errorf("%w: invalid MLSD line %q", ErrProtocol, line)
	}
	factsText := line[:separator]
	name := strings.TrimSpace(line[separator+1:])
	if name == "" {
		return DirEntry{}, fmt.Errorf("%w: empty MLSD name", ErrProtocol)
	}
	entry := DirEntry{Name: name}
	for _, fact := range strings.Split(factsText, ";") {
		if fact == "" {
			continue
		}
		pair := strings.SplitN(fact, "=", 2)
		if len(pair) != 2 {
			continue
		}
		key := strings.ToLower(pair[0])
		value := pair[1]
		switch key {
		case "type":
			entry.Info.IsDir = strings.EqualFold(value, "dir") || strings.EqualFold(value, "cdir") || strings.EqualFold(value, "pdir")
		case "size":
			size, err := strconv.ParseInt(value, 10, 64)
			if err != nil || size < 0 {
				return DirEntry{}, fmt.Errorf("%w: invalid MLSD size %q", ErrProtocol, value)
			}
			entry.Info.Size = size
		case "modify":
			if len(value) >= 14 {
				if parsed, err := time.Parse("20060102150405", value[:14]); err == nil {
					entry.Info.ModUnix = parsed.UTC().Unix()
				}
			}
		}
	}
	return entry, nil
}
