package export

import (
	"fmt"
	"strings"
)

// PlanFile 把一个由稳定 routing 身份产生的文件加入 Manifest；重复调用同一路径是幂等的。
func (m *Manifest) PlanFile(filePath string) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return fmt.Errorf("%w: empty file path", ErrInvalidManifest)
	}
	if m.ExportState != ExportStateWriting {
		return fmt.Errorf("%w: cannot plan file while export is %s", ErrInvalidTransition, m.ExportState)
	}
	for i := range m.FileStates {
		if m.FileStates[i].Path == filePath {
			return nil
		}
	}
	m.FileStates = append(m.FileStates, FileEntry{Path: filePath, State: FileStatePlanned})
	return nil
}

// LookupFile 返回 Manifest 中指定文件的当前持久化状态副本。
func (m *Manifest) LookupFile(filePath string) (FileEntry, bool) {
	if m == nil {
		return FileEntry{}, false
	}
	for i := range m.FileStates {
		if m.FileStates[i].Path == filePath {
			return m.FileStates[i], true
		}
	}
	return FileEntry{}, false
}
