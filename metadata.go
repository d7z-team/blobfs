package blobfs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/afero"
)

const (
	fileStateActive  = "ACTIVE"
	fileStateDeleted = "DELETED"

	manifestStateActive  = "ACTIVE"
	manifestStateDeleted = "DELETED"

	chunkStateWriting          = "WRITING"
	chunkStateActive           = "ACTIVE"
	chunkStateCorrupt          = "CORRUPT"
	chunkStateGarbageCandidate = "GARBAGE_CANDIDATE"
	chunkStateDeleting         = "DELETING"
	chunkStateDeleted          = "DELETED"

	segmentStateOpen       = "OPEN"
	segmentStateSealed     = "SEALED"
	segmentStateCorrupt    = "CORRUPT"
	segmentStateCompacting = "COMPACTING"
	segmentStateCompacted  = "COMPACTED"
	segmentStateDeleted    = "DELETED"

	chunkingSingle  = "SINGLE"
	chunkingFastCDC = "FASTCDC"

	fileKindFile    = "FILE"
	fileKindDir     = "DIR"
	fileKindSymlink = "SYMLINK"
)

type fileRecord struct {
	FileID             string            `json:"file_id"`
	TenantID           string            `json:"tenant_id"`
	Path               string            `json:"path"`
	ParentPath         string            `json:"parent_path"`
	Name               string            `json:"name"`
	Kind               string            `json:"kind"`
	Size               int64             `json:"size"`
	FileHash           string            `json:"file_hash,omitempty"`
	ManifestID         string            `json:"manifest_id,omitempty"`
	State              string            `json:"state"`
	Options            map[string]string `json:"options,omitempty"`
	Mode               uint32            `json:"mode,omitempty"`
	ModTime            int64             `json:"mod_time,omitempty"`
	UID                int               `json:"uid,omitempty"`
	GID                int               `json:"gid,omitempty"`
	LinkTarget         string            `json:"link_target,omitempty"`
	Generation         uint64            `json:"generation"`
	ContentGeneration  uint64            `json:"content_generation,omitempty"`
	MetadataGeneration uint64            `json:"metadata_generation,omitempty"`
	CTime              int64             `json:"ctime,omitempty"`
	MTime              int64             `json:"mtime,omitempty"`
	ATime              int64             `json:"atime,omitempty"`
	CreatedAt          int64             `json:"created_at"`
	UpdatedAt          int64             `json:"updated_at"`
	DeletedAt          int64             `json:"deleted_at,omitempty"`
}

type manifestRecord struct {
	ManifestID   string          `json:"manifest_id"`
	TenantID     string          `json:"tenant_id"`
	FileSize     int64           `json:"file_size"`
	FileHash     string          `json:"file_hash"`
	ChunkCount   int             `json:"chunk_count"`
	ChunkingType string          `json:"chunking_type"`
	State        string          `json:"state"`
	Chunks       []manifestChunk `json:"chunks,omitempty"`
	CreatedAt    int64           `json:"created_at"`
	LastLiveAt   int64           `json:"last_live_at,omitempty"`
	DeletedAt    int64           `json:"deleted_at,omitempty"`
}

type manifestChunk struct {
	ManifestID string `json:"manifest_id,omitempty"`
	Index      int    `json:"chunk_index"`
	ChunkID    string `json:"chunk_id"`
	FileOffset int64  `json:"file_offset"`
	ChunkSize  int64  `json:"chunk_size"`
}

type chunkRecord struct {
	ChunkID            string `json:"chunk_id"`
	TenantID           string `json:"tenant_id"`
	RawSize            int64  `json:"raw_size"`
	StoredSize         int64  `json:"stored_size"`
	State              string `json:"state"`
	SegmentID          string `json:"segment_id,omitempty"`
	SegmentOffset      int64  `json:"segment_offset,omitempty"`
	SegmentLength      int64  `json:"segment_length,omitempty"`
	ChecksumCRC32C     uint32 `json:"checksum_crc32c,omitempty"`
	Compression        string `json:"compression,omitempty"`
	CreatedAt          int64  `json:"created_at"`
	LastSeenAt         int64  `json:"last_seen_at"`
	LastLiveEpoch      int64  `json:"last_live_epoch,omitempty"`
	GarbageSeenCount   int    `json:"garbage_seen_count,omitempty"`
	GarbageCandidateAt int64  `json:"garbage_candidate_at,omitempty"`
	DeletingAt         int64  `json:"deleting_at,omitempty"`
	CorruptAt          int64  `json:"corrupt_at,omitempty"`
	CorruptReason      string `json:"corrupt_reason,omitempty"`
	DeletedAt          int64  `json:"deleted_at,omitempty"`
}

type segmentRecord struct {
	SegmentID            string `json:"segment_id"`
	RelativePath         string `json:"relative_path"`
	WriteOffset          int64  `json:"write_offset"`
	TotalBytes           int64  `json:"total_bytes"`
	LiveBytesEstimate    int64  `json:"live_bytes_estimate"`
	GarbageBytesEstimate int64  `json:"garbage_bytes_estimate"`
	State                string `json:"state"`
	CreatedAt            int64  `json:"created_at"`
	SealedAt             int64  `json:"sealed_at,omitempty"`
	CompactedAt          int64  `json:"compacted_at,omitempty"`
	CorruptAt            int64  `json:"corrupt_at,omitempty"`
	CorruptReason        string `json:"corrupt_reason,omitempty"`
	DeletedAt            int64  `json:"deleted_at,omitempty"`
}

type gcRun struct {
	Epoch        int64  `json:"epoch"`
	State        string `json:"state"`
	StartedAt    int64  `json:"started_at"`
	FinishedAt   int64  `json:"finished_at,omitempty"`
	SafetyCutoff int64  `json:"safety_cutoff"`
	Notes        string `json:"notes,omitempty"`
}

type metadata struct {
	Version        int                          `json:"version"`
	NextFileSeq    int64                        `json:"next_file_seq"`
	NextSegmentSeq int64                        `json:"next_segment_seq"`
	NextGCEpoch    int64                        `json:"next_gc_epoch"`
	Files          map[string]*fileRecord       `json:"files"`
	DirEntries     map[string]map[string]string `json:"dir_entries"`
	Manifests      map[string]*manifestRecord   `json:"manifests"`
	Chunks         map[string]*chunkRecord      `json:"chunks"`
	Segments       map[string]*segmentRecord    `json:"segments"`
	GCRuns         []gcRun                      `json:"gc_runs,omitempty"`
}

func newMetadata() *metadata {
	return &metadata{
		Version:        1,
		NextFileSeq:    1,
		NextSegmentSeq: 1,
		NextGCEpoch:    1,
		Files:          map[string]*fileRecord{},
		DirEntries:     map[string]map[string]string{},
		Manifests:      map[string]*manifestRecord{},
		Chunks:         map[string]*chunkRecord{},
		Segments:       map[string]*segmentRecord{},
	}
}

func loadMetadata(fs afero.Fs, path string) (*metadata, error) {
	data, err := afero.ReadFile(fs, path)
	if os.IsNotExist(err) {
		return newMetadata(), nil
	}
	if err != nil {
		return nil, err
	}
	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	if meta.Files == nil {
		meta.Files = map[string]*fileRecord{}
	}
	if meta.DirEntries == nil {
		meta.DirEntries = map[string]map[string]string{}
	}
	if meta.Manifests == nil {
		meta.Manifests = map[string]*manifestRecord{}
	}
	if meta.Chunks == nil {
		meta.Chunks = map[string]*chunkRecord{}
	}
	if meta.Segments == nil {
		meta.Segments = map[string]*segmentRecord{}
	}
	if meta.NextFileSeq == 0 {
		meta.NextFileSeq = 1
	}
	if meta.NextSegmentSeq == 0 {
		meta.NextSegmentSeq = 1
	}
	if meta.NextGCEpoch == 0 {
		meta.NextGCEpoch = 1
	}
	rebuildDirEntries(&meta)
	return &meta, nil
}

func rebuildDirEntries(meta *metadata) {
	meta.DirEntries = map[string]map[string]string{}
	for key, record := range meta.Files {
		if record == nil {
			delete(meta.Files, key)
			continue
		}
		if record.Kind == "" {
			if record.Mode&uint32(os.ModeDir) != 0 {
				record.Kind = fileKindDir
			} else {
				record.Kind = fileKindFile
			}
		}
		if record.ParentPath == "" && record.Path != "" {
			record.ParentPath = parentPath(record.Path)
		}
		if record.Name == "" && record.Path != "" {
			record.Name = pathBase(record.Path)
		}
		if record.Generation == 0 {
			record.Generation = 1
		}
		if record.ContentGeneration == 0 && record.Kind == fileKindFile {
			record.ContentGeneration = record.Generation
		}
		if record.MetadataGeneration == 0 {
			record.MetadataGeneration = record.Generation
		}
		if record.CTime == 0 {
			record.CTime = record.CreatedAt
		}
		if record.MTime == 0 {
			record.MTime = record.ModTime
			if record.MTime == 0 {
				record.MTime = record.UpdatedAt
			}
		}
		if record.State != fileStateActive || record.Path == "" {
			continue
		}
		parentKey := dirKey(record.TenantID, record.ParentPath)
		if meta.DirEntries[parentKey] == nil {
			meta.DirEntries[parentKey] = map[string]string{}
		}
		meta.DirEntries[parentKey][record.Name] = fileKey(record.TenantID, record.Path)
	}
}

func saveMetadata(fs afero.Fs, path string, meta *metadata) error {
	if err := fs.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	temp, tempName, err := createMetadataTemp(fs, filepath.Dir(path))
	if err != nil {
		return err
	}
	if _, err = temp.Write(data); err != nil {
		_ = temp.Close()
		_ = fs.Remove(tempName)
		return err
	}
	if err = temp.Sync(); err != nil {
		_ = temp.Close()
		_ = fs.Remove(tempName)
		return err
	}
	if err = temp.Close(); err != nil {
		_ = fs.Remove(tempName)
		return err
	}
	if err = fs.Rename(tempName, path); err != nil {
		_ = fs.Remove(tempName)
		return err
	}
	return nil
}

func createMetadataTemp(fs afero.Fs, dir string) (afero.File, string, error) {
	for i := 0; i < 100; i++ {
		name := filepath.Join(dir, fmt.Sprintf(".blobfs-meta-%d-%d", nowUnix(), i))
		file, err := fs.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if os.IsExist(err) {
			continue
		}
		return file, name, err
	}
	return nil, "", fmt.Errorf("create metadata temp file in %s: exhausted name attempts", dir)
}

func nowUnix() int64 {
	return time.Now().UnixNano()
}
