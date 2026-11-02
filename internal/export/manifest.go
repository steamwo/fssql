package export

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const CurrentManifestVersion = 1

var (
	ErrInvalidManifest   = errors.New("export: invalid manifest")
	ErrInvalidTransition = errors.New("export: invalid state transition")
)

// ExportState 表示一次导出作业的持久化生命周期状态。
type ExportState string

const (
	ExportStatePlanned    ExportState = "PLANNED"
	ExportStateWriting    ExportState = "WRITING"
	ExportStateCommitting ExportState = "COMMITTING"
	ExportStateCommitted  ExportState = "COMMITTED"
	ExportStateFailed     ExportState = "FAILED"
)

// FileState 表示 Manifest 中单个目标文件的持久化状态。
type FileState string

const (
	FileStatePlanned  FileState = "PLANNED"
	FileStateWriting  FileState = "WRITING"
	FileStateComplete FileState = "COMPLETE"
	FileStateFailed   FileState = "FAILED"
)

// SourceSnapshotRef 保存导出开始时冻结的源数据快照身份。
type SourceSnapshotRef struct {
	// Fingerprint 是不可变 ScanSet/SourceSnapshot 的稳定指纹。
	Fingerprint string `json:"fingerprint"`
}

// Destination 描述导出的逻辑目标；协议细节仍由 VFS 驱动负责。
type Destination struct {
	// URI 是最终目标 URI 或数据集前缀。
	URI string `json:"uri"`
}

// PublishedObjectIdentity 是 single-object publish 后用于 crash reconciliation 的稳定内部身份。
// 它是 Manifest recovery metadata，不是 SQL/public storage sidecar contract。
type PublishedObjectIdentity struct {
	ETag      string `json:"etag,omitempty"`
	VersionID string `json:"version_id,omitempty"`
	Size      int64  `json:"size"`
}

// Stable 表示该 identity 至少包含一个协议可验证的稳定 token。
func (i PublishedObjectIdentity) Stable() bool { return i.ETag != "" || i.VersionID != "" }

// FileEntry 描述一个由 Manifest 管理的目标文件。
type FileEntry struct {
	// Path 是相对于目标数据集根目录的稳定文件名。
	Path string `json:"path"`
	// State 是文件当前生命周期状态。
	State FileState `json:"state"`
	// Size 是 COMPLETE 文件已经确认写入的字节数。
	Size int64 `json:"size,omitempty"`
	// Checksum 是可选的完整文件内容校验值。
	Checksum string `json:"checksum,omitempty"`
	// PublishedIdentity 仅在 VFS 已成功发布并返回对象身份后记录；缺失时恢复不得猜测已提交。
	PublishedIdentity *PublishedObjectIdentity `json:"published_identity,omitempty"`
}

// Manifest 是 Phase 4 导出恢复与提交的唯一持久化作业状态载体。
type Manifest struct {
	// ManifestVersion 控制持久化格式兼容性。
	ManifestVersion int `json:"manifest_version"`
	// JobID 唯一标识一次逻辑导出作业。
	JobID string `json:"job_id"`
	// QueryFingerprint 标识产生输出的绑定后查询身份。
	QueryFingerprint string `json:"query_fingerprint"`
	// SourceSnapshot 固定本次作业读取的数据集身份。
	SourceSnapshot SourceSnapshotRef `json:"source_snapshot"`
	// Destination 描述最终输出位置。
	Destination Destination `json:"destination"`
	// Format 是目标逻辑格式，例如 parquet/csv/ndjson。
	Format string `json:"format"`
	// Compression 是独立于协议和格式的压缩层名称。
	Compression string `json:"compression"`
	// Consistency 记录本次导出采用的一致性策略。
	Consistency string `json:"consistency"`
	// SegmentPlan 保存 resumable MultiFile/Partitioned 的稳定 segment identity；SingleFile 可为空。
	SegmentPlan *SegmentPlan `json:"segment_plan,omitempty"`
	// FileStates 保存所有计划输出文件的状态；已完成文件不可回退。
	FileStates []FileEntry `json:"file_states"`
	// ExportState 是整个导出作业的状态。
	ExportState ExportState `json:"export_state"`
}

// NewManifest 构造处于 PLANNED 状态的 v1 Manifest，并立即校验必填身份字段。
func NewManifest(jobID, queryFingerprint, sourceFingerprint, destinationURI, format, compression, consistency string, files []string) (*Manifest, error) {
	m := &Manifest{
		ManifestVersion:  CurrentManifestVersion,
		JobID:            strings.TrimSpace(jobID),
		QueryFingerprint: strings.TrimSpace(queryFingerprint),
		SourceSnapshot:   SourceSnapshotRef{Fingerprint: strings.TrimSpace(sourceFingerprint)},
		Destination:      Destination{URI: strings.TrimSpace(destinationURI)},
		Format:           strings.ToLower(strings.TrimSpace(format)),
		Compression:      strings.ToLower(strings.TrimSpace(compression)),
		Consistency:      strings.TrimSpace(consistency),
		ExportState:      ExportStatePlanned,
		FileStates:       make([]FileEntry, len(files)),
	}
	for i, file := range files {
		m.FileStates[i] = FileEntry{Path: strings.TrimSpace(file), State: FileStatePlanned}
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// Validate 检查 Manifest 的版本、身份字段、SegmentPlan、文件唯一性以及状态组合是否自洽。
func (m *Manifest) Validate() error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	if m.ManifestVersion != CurrentManifestVersion {
		return fmt.Errorf("%w: unsupported manifest version %d", ErrInvalidManifest, m.ManifestVersion)
	}
	if strings.TrimSpace(m.JobID) == "" || strings.TrimSpace(m.QueryFingerprint) == "" || strings.TrimSpace(m.SourceSnapshot.Fingerprint) == "" {
		return fmt.Errorf("%w: missing job/query/source identity", ErrInvalidManifest)
	}
	if strings.TrimSpace(m.Destination.URI) == "" || strings.TrimSpace(m.Format) == "" || strings.TrimSpace(m.Consistency) == "" {
		return fmt.Errorf("%w: missing destination/format/consistency", ErrInvalidManifest)
	}
	if m.SegmentPlan != nil {
		if err := m.SegmentPlan.Validate(); err != nil {
			return err
		}
	}
	if !validExportState(m.ExportState) {
		return fmt.Errorf("%w: unknown export state %q", ErrInvalidManifest, m.ExportState)
	}
	seen := make(map[string]struct{}, len(m.FileStates))
	for i := range m.FileStates {
		f := m.FileStates[i]
		if strings.TrimSpace(f.Path) == "" {
			return fmt.Errorf("%w: file %d has empty path", ErrInvalidManifest, i)
		}
		if _, ok := seen[f.Path]; ok {
			return fmt.Errorf("%w: duplicate file path %q", ErrInvalidManifest, f.Path)
		}
		seen[f.Path] = struct{}{}
		if !validFileState(f.State) {
			return fmt.Errorf("%w: file %q has unknown state %q", ErrInvalidManifest, f.Path, f.State)
		}
		if f.Size < 0 {
			return fmt.Errorf("%w: file %q has negative size", ErrInvalidManifest, f.Path)
		}
		if f.PublishedIdentity != nil {
			if f.State != FileStateComplete {
				return fmt.Errorf("%w: file %q has published identity before COMPLETE", ErrInvalidManifest, f.Path)
			}
			if f.PublishedIdentity.Size < 0 || f.PublishedIdentity.Size != f.Size {
				return fmt.Errorf("%w: file %q published identity size mismatch", ErrInvalidManifest, f.Path)
			}
		}
	}
	if m.ExportState == ExportStateCommitted && !m.AllFilesComplete() {
		return fmt.Errorf("%w: committed export contains incomplete files", ErrInvalidManifest)
	}
	return nil
}

// TransitionExport 将作业切换到下一个合法状态；COMMITTED 是不可逆终态。
func (m *Manifest) TransitionExport(next ExportState) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidTransition)
	}
	if !canTransitionExport(m.ExportState, next) {
		return fmt.Errorf("%w: export %s -> %s", ErrInvalidTransition, m.ExportState, next)
	}
	if next == ExportStateCommitting && !m.AllFilesComplete() {
		return fmt.Errorf("%w: cannot commit before every file is complete", ErrInvalidTransition)
	}
	m.ExportState = next
	return nil
}

// TransitionFile 更新一个目标文件的状态；COMPLETE 文件不可重新进入写入态。
func (m *Manifest) TransitionFile(path string, next FileState, size int64, checksum string) error {
	return m.transitionFile(path, next, size, checksum, nil)
}

// TransitionFilePublished 把成功 VFS publish 返回的稳定对象身份与 COMPLETE 状态一起记录。
func (m *Manifest) TransitionFilePublished(path string, size int64, checksum string, identity PublishedObjectIdentity) error {
	if !identity.Stable() {
		return fmt.Errorf("%w: published identity is not stable", ErrInvalidTransition)
	}
	if identity.Size != size {
		return fmt.Errorf("%w: published identity size %d != file size %d", ErrInvalidTransition, identity.Size, size)
	}
	copy := identity
	return m.transitionFile(path, FileStateComplete, size, checksum, &copy)
}

func (m *Manifest) transitionFile(path string, next FileState, size int64, checksum string, identity *PublishedObjectIdentity) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidTransition)
	}
	if size < 0 {
		return fmt.Errorf("%w: negative file size", ErrInvalidTransition)
	}
	for i := range m.FileStates {
		f := &m.FileStates[i]
		if f.Path != path {
			continue
		}
		if !canTransitionFile(f.State, next) {
			return fmt.Errorf("%w: file %q %s -> %s", ErrInvalidTransition, path, f.State, next)
		}
		f.State = next
		if next == FileStateComplete {
			f.Size = size
			f.Checksum = checksum
			f.PublishedIdentity = identity
		} else {
			// 未完成文件不能携带上一次尝试留下的完成元数据，恢复时会整文件重做。
			f.Size = 0
			f.Checksum = ""
			f.PublishedIdentity = nil
		}
		return nil
	}
	return fmt.Errorf("%w: unknown file %q", ErrInvalidTransition, path)
}

// AllFilesComplete 判断当前计划中的每个文件是否都已进入 COMPLETE 状态。
func (m *Manifest) AllFilesComplete() bool {
	if m == nil || len(m.FileStates) == 0 {
		return false
	}
	for i := range m.FileStates {
		if m.FileStates[i].State != FileStateComplete {
			return false
		}
	}
	return true
}

// MarshalBinary 生成稳定 JSON 表示，用于持久化到 staging Manifest。
func (m *Manifest) MarshalBinary() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

// UnmarshalManifest 读取持久化 Manifest，并在返回前拒绝版本、SegmentPlan 或状态损坏。
func UnmarshalManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", ErrInvalidManifest, err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

func validExportState(s ExportState) bool {
	switch s {
	case ExportStatePlanned, ExportStateWriting, ExportStateCommitting, ExportStateCommitted, ExportStateFailed:
		return true
	default:
		return false
	}
}

func validFileState(s FileState) bool {
	switch s {
	case FileStatePlanned, FileStateWriting, FileStateComplete, FileStateFailed:
		return true
	default:
		return false
	}
}

func canTransitionExport(from, to ExportState) bool {
	switch from {
	case ExportStatePlanned:
		return to == ExportStateWriting || to == ExportStateFailed
	case ExportStateWriting:
		return to == ExportStateCommitting || to == ExportStateFailed
	case ExportStateCommitting:
		return to == ExportStateCommitted || to == ExportStateFailed
	case ExportStateFailed:
		// FAILED 可以恢复到 WRITING；恢复粒度仍由文件状态决定，已完成文件不回退。
		return to == ExportStateWriting
	default:
		return false
	}
}

func canTransitionFile(from, to FileState) bool {
	switch from {
	case FileStatePlanned:
		return to == FileStateWriting || to == FileStateFailed
	case FileStateWriting:
		return to == FileStateComplete || to == FileStateFailed
	case FileStateFailed:
		// Incomplete file 在恢复时整文件重做，所以从 FAILED 重新进入 WRITING。
		return to == FileStateWriting
	default:
		return false
	}
}
