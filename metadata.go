package blobfs

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/afero"
)

const (
	fileStateActive  = "ACTIVE"
	fileStateDeleted = "DELETED"

	manifestStateActive  = "ACTIVE"
	manifestStateDeleted = "DELETED"

	chunkStateActive           = "ACTIVE"
	chunkStateCorrupt          = "CORRUPT"
	chunkStateGarbageCandidate = "GARBAGE_CANDIDATE"
	chunkStateDeleted          = "DELETED"

	segmentStateSealed     = "SEALED"
	segmentStateCorrupt    = "CORRUPT"
	segmentStateCompacting = "COMPACTING"
	segmentStateDeleted    = "DELETED"

	chunkingSingle  = "SINGLE"
	chunkingFastCDC = "FASTCDC"

	fileKindFile = "FILE"
	fileKindDir  = "DIR"
)

const (
	metaFormatVersion      = 2
	metaLogFile            = "000001.log"
	metaCheckpointFile     = "checkpoint.json"
	metaCheckpointInterval = 128
	metaFrameMagic         = uint32(0x324d4642)
	maxRecentGCRuns        = 1024
)

type inodeRecord struct {
	InodeID             uint64            `json:"inode_id"`
	TenantID            string            `json:"tenant_id"`
	Kind                string            `json:"kind"`
	ParentInode         uint64            `json:"parent_inode,omitempty"`
	Name                string            `json:"name"`
	Size                int64             `json:"size,omitempty"`
	FileHash            string            `json:"file_hash,omitempty"`
	ManifestID          string            `json:"manifest_id,omitempty"`
	State               string            `json:"state"`
	Options             map[string]string `json:"options,omitempty"`
	Mode                uint32            `json:"mode,omitempty"`
	ModTime             int64             `json:"mod_time,omitempty"`
	UID                 int               `json:"uid,omitempty"`
	GID                 int               `json:"gid,omitempty"`
	Generation          uint64            `json:"generation"`
	ContentGeneration   uint64            `json:"content_generation,omitempty"`
	MetadataGeneration  uint64            `json:"metadata_generation,omitempty"`
	NamespaceGeneration uint64            `json:"namespace_generation,omitempty"`
	CTime               int64             `json:"ctime,omitempty"`
	MTime               int64             `json:"mtime,omitempty"`
	ATime               int64             `json:"atime,omitempty"`
	CreatedAt           int64             `json:"created_at"`
	UpdatedAt           int64             `json:"updated_at"`
	DeletedAt           int64             `json:"deleted_at,omitempty"`
}

type manifestRecord struct {
	ManifestID   string          `json:"manifest_id"`
	TenantID     string          `json:"tenant_id"`
	FileSize     int64           `json:"file_size"`
	FileHash     string          `json:"file_hash"`
	ChunkCount   int             `json:"chunk_count"`
	ChunkingType string          `json:"chunking_type"`
	State        string          `json:"state"`
	RefCount     int             `json:"ref_count"`
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
	RefCount           int    `json:"ref_count"`
	State              string `json:"state"`
	SegmentID          string `json:"segment_id,omitempty"`
	SegmentOffset      int64  `json:"segment_offset,omitempty"`
	SegmentLength      int64  `json:"segment_length,omitempty"`
	ChecksumCRC32C     uint32 `json:"checksum_crc32c,omitempty"`
	Compression        string `json:"compression,omitempty"`
	CreatedAt          int64  `json:"created_at"`
	LastSeenAt         int64  `json:"last_seen_at"`
	GarbageSeenCount   int    `json:"garbage_seen_count,omitempty"`
	GarbageCandidateAt int64  `json:"garbage_candidate_at,omitempty"`
	CorruptAt          int64  `json:"corrupt_at,omitempty"`
	CorruptReason      string `json:"corrupt_reason,omitempty"`
	DeletedAt          int64  `json:"deleted_at,omitempty"`
}

type segmentRecord struct {
	SegmentID     string `json:"segment_id"`
	RelativePath  string `json:"relative_path"`
	WriteOffset   int64  `json:"write_offset"`
	TotalBytes    int64  `json:"total_bytes"`
	State         string `json:"state"`
	CreatedAt     int64  `json:"created_at"`
	SealedAt      int64  `json:"sealed_at,omitempty"`
	CompactedAt   int64  `json:"compacted_at,omitempty"`
	CorruptAt     int64  `json:"corrupt_at,omitempty"`
	CorruptReason string `json:"corrupt_reason,omitempty"`
	DeletedAt     int64  `json:"deleted_at,omitempty"`
}

type gcRun struct {
	Epoch        int64  `json:"epoch"`
	State        string `json:"state"`
	StartedAt    int64  `json:"started_at"`
	FinishedAt   int64  `json:"finished_at,omitempty"`
	SafetyCutoff int64  `json:"safety_cutoff"`
	Notes        string `json:"notes,omitempty"`
}

type gcMetadata struct {
	TotalRuns int64   `json:"total_runs"`
	LastEpoch int64   `json:"last_epoch"`
	Recent    []gcRun `json:"recent,omitempty"`
}

type metadata struct {
	Version        int                          `json:"version"`
	TxID           uint64                       `json:"txid"`
	NextInodeID    uint64                       `json:"next_inode_id"`
	NextSegmentSeq int64                        `json:"next_segment_seq"`
	NextGCEpoch    int64                        `json:"next_gc_epoch"`
	Tenants        map[string]uint64            `json:"tenants"`
	Inodes         map[uint64]*inodeRecord      `json:"inodes"`
	DirEntries     map[uint64]map[string]uint64 `json:"dir_entries"`
	Manifests      map[string]*manifestRecord   `json:"manifests"`
	Chunks         map[string]*chunkRecord      `json:"chunks"`
	Segments       map[string]*segmentRecord    `json:"segments"`
	GC             gcMetadata                   `json:"gc,omitempty"`
}

type metaTx struct {
	TxID uint64   `json:"txid"`
	Ops  []metaOp `json:"ops"`
}

type metaOp struct {
	Type     string          `json:"type"`
	TenantID string          `json:"tenant_id,omitempty"`
	ParentID uint64          `json:"parent_id,omitempty"`
	Name     string          `json:"name,omitempty"`
	ChildID  uint64          `json:"child_id,omitempty"`
	Inode    *inodeRecord    `json:"inode,omitempty"`
	Manifest *manifestRecord `json:"manifest,omitempty"`
	Chunk    *chunkRecord    `json:"chunk,omitempty"`
	Segment  *segmentRecord  `json:"segment,omitempty"`
	GCRun    *gcRun          `json:"gc_run,omitempty"`
}

type metaSuperBlock struct {
	FormatVersion  int    `json:"format_version"`
	CheckpointTxID uint64 `json:"checkpoint_txid"`
	LogFile        string `json:"log_file"`
	CRC            uint32 `json:"crc"`
}

func newMetadata() *metadata {
	return &metadata{
		Version:        metaFormatVersion,
		NextInodeID:    1,
		NextSegmentSeq: 1,
		NextGCEpoch:    1,
		Tenants:        map[string]uint64{},
		Inodes:         map[uint64]*inodeRecord{},
		DirEntries:     map[uint64]map[string]uint64{},
		Manifests:      map[string]*manifestRecord{},
		Chunks:         map[string]*chunkRecord{},
		Segments:       map[string]*segmentRecord{},
	}
}

func loadMetadata(fs afero.Fs, metaDir string) (*metadata, string, error) {
	meta := newMetadata()
	if err := fs.MkdirAll(filepath.Join(metaDir, "txlog"), 0o755); err != nil {
		return nil, "", err
	}
	if err := loadMetaCheckpoint(fs, filepath.Join(metaDir, metaCheckpointFile), meta); err != nil {
		return nil, "", err
	}
	super, err := loadMetaSuperBlock(fs, metaDir)
	if err != nil {
		return nil, "", err
	}
	logFile := super.LogFile
	if logFile == "" {
		logFile = metaLogFile
	}
	if err := replayMetaLog(fs, filepath.Join(metaDir, "txlog", logFile), meta); err != nil {
		return nil, "", err
	}
	recoverInProgressMetadata(meta)
	recomputeMetaCounters(meta)
	return meta, logFile, nil
}

func loadMetaCheckpoint(fs afero.Fs, path string, meta *metadata) error {
	data, err := afero.ReadFile(fs, path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, meta); err != nil {
		return err
	}
	ensureMetaMaps(meta)
	return nil
}

func replayMetaLog(fs afero.Fs, path string, meta *metadata) error {
	file, err := fs.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		var header [12]byte
		if _, err := io.ReadFull(file, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		if binary.LittleEndian.Uint32(header[0:4]) != metaFrameMagic {
			return errors.New("invalid metadata log frame magic")
		}
		size := binary.LittleEndian.Uint32(header[4:8])
		wantCRC := binary.LittleEndian.Uint32(header[8:12])
		if size == 0 || size > 64<<20 {
			return errors.New("invalid metadata log frame size")
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(file, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return errors.New("metadata log frame checksum mismatch")
		}
		var tx metaTx
		if err := json.Unmarshal(payload, &tx); err != nil {
			return err
		}
		if tx.TxID <= meta.TxID {
			continue
		}
		applyMetaTx(meta, tx)
	}
}

func writeMetaTx(file afero.File, tx metaTx) error {
	if file == nil {
		return errors.New("metadata log is not open")
	}
	payload, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	var header [12]byte
	binary.LittleEndian.PutUint32(header[0:4], metaFrameMagic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[8:12], crc32.ChecksumIEEE(payload))
	if _, err := file.Write(header[:]); err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		return err
	}
	return file.Sync()
}

func applyMetaTx(meta *metadata, tx metaTx) {
	for _, op := range tx.Ops {
		applyMetaOp(meta, op)
	}
	if tx.TxID > meta.TxID {
		meta.TxID = tx.TxID
	}
}

func applyMetaOp(meta *metadata, op metaOp) {
	switch op.Type {
	case "put_tenant":
		meta.Tenants[op.TenantID] = op.ChildID
	case "put_inode":
		if op.Inode != nil {
			inode := *op.Inode
			inode.Options = copyOptions(inode.Options)
			meta.Inodes[inode.InodeID] = &inode
		}
	case "put_dirent":
		if meta.DirEntries[op.ParentID] == nil {
			meta.DirEntries[op.ParentID] = map[string]uint64{}
		}
		meta.DirEntries[op.ParentID][op.Name] = op.ChildID
	case "delete_dirent":
		if entries := meta.DirEntries[op.ParentID]; entries != nil {
			delete(entries, op.Name)
			if len(entries) == 0 {
				delete(meta.DirEntries, op.ParentID)
			}
		}
	case "put_manifest":
		if op.Manifest != nil {
			manifest := *op.Manifest
			manifest.Chunks = append([]manifestChunk(nil), op.Manifest.Chunks...)
			meta.Manifests[manifest.ManifestID] = &manifest
		}
	case "put_chunk":
		if op.Chunk != nil {
			chunk := *op.Chunk
			meta.Chunks[chunk.ChunkID] = &chunk
		}
	case "put_segment":
		if op.Segment != nil {
			seg := *op.Segment
			meta.Segments[seg.SegmentID] = &seg
		}
	case "append_gcrun":
		if op.GCRun != nil {
			meta.GC.TotalRuns++
			if op.GCRun.Epoch > meta.GC.LastEpoch {
				meta.GC.LastEpoch = op.GCRun.Epoch
			}
			meta.GC.Recent = append(meta.GC.Recent, *op.GCRun)
			trimRecentGCRuns(meta)
		}
	}
}

func recomputeMetaCounters(meta *metadata) {
	meta.Version = metaFormatVersion
	ensureMetaMaps(meta)
	if meta.NextInodeID == 0 {
		meta.NextInodeID = 1
	}
	if meta.NextSegmentSeq == 0 {
		meta.NextSegmentSeq = 1
	}
	if meta.NextGCEpoch == 0 {
		meta.NextGCEpoch = 1
	}
	for id := range meta.Inodes {
		if id >= meta.NextInodeID {
			meta.NextInodeID = id + 1
		}
	}
	for _, seg := range meta.Segments {
		var seq int64
		_, _ = sscanfSegmentID(seg.SegmentID, &seq)
		if seq >= meta.NextSegmentSeq {
			meta.NextSegmentSeq = seq + 1
		}
	}
	if meta.GC.TotalRuns < int64(len(meta.GC.Recent)) {
		meta.GC.TotalRuns = int64(len(meta.GC.Recent))
	}
	for _, run := range meta.GC.Recent {
		if run.Epoch >= meta.NextGCEpoch {
			meta.NextGCEpoch = run.Epoch + 1
		}
		if run.Epoch > meta.GC.LastEpoch {
			meta.GC.LastEpoch = run.Epoch
		}
	}
	if meta.GC.LastEpoch >= meta.NextGCEpoch {
		meta.NextGCEpoch = meta.GC.LastEpoch + 1
	}
	trimRecentGCRuns(meta)
}

func ensureMetaMaps(meta *metadata) {
	if meta.Tenants == nil {
		meta.Tenants = map[string]uint64{}
	}
	if meta.Inodes == nil {
		meta.Inodes = map[uint64]*inodeRecord{}
	}
	if meta.DirEntries == nil {
		meta.DirEntries = map[uint64]map[string]uint64{}
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
}

func recoverInProgressMetadata(meta *metadata) {
	for _, seg := range meta.Segments {
		if seg.State == segmentStateCompacting {
			seg.State = segmentStateSealed
		}
	}
}

func saveMetaCheckpoint(fs afero.Fs, metaDir string, meta *metadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return writeFileAtomicSync(fs, filepath.Join(metaDir, metaCheckpointFile), data, 0o600)
}

func loadMetaSuperBlock(fs afero.Fs, metaDir string) (metaSuperBlock, error) {
	var best metaSuperBlock
	var haveValid bool
	var firstInvalid error
	for _, name := range []string{"SUPER0", "SUPER1"} {
		data, err := afero.ReadFile(fs, filepath.Join(metaDir, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return metaSuperBlock{}, err
		}
		super, err := decodeMetaSuperBlock(data)
		if err != nil {
			if firstInvalid == nil {
				firstInvalid = err
			}
			continue
		}
		if !haveValid || super.CheckpointTxID >= best.CheckpointTxID {
			best = super
			haveValid = true
		}
	}
	if haveValid {
		return best, nil
	}
	if firstInvalid != nil {
		return metaSuperBlock{}, firstInvalid
	}
	return metaSuperBlock{FormatVersion: metaFormatVersion, LogFile: metaLogFile}, nil
}

func decodeMetaSuperBlock(data []byte) (metaSuperBlock, error) {
	var super metaSuperBlock
	if err := json.Unmarshal(data, &super); err != nil {
		return metaSuperBlock{}, err
	}
	if super.FormatVersion != metaFormatVersion {
		return metaSuperBlock{}, errors.New("unsupported metadata superblock version")
	}
	if super.LogFile == "" || filepath.Base(super.LogFile) != super.LogFile || !strings.HasSuffix(super.LogFile, ".log") {
		return metaSuperBlock{}, errors.New("invalid metadata superblock log file")
	}
	wantCRC := super.CRC
	super.CRC = 0
	payload, err := json.Marshal(super)
	if err != nil {
		return metaSuperBlock{}, err
	}
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return metaSuperBlock{}, errors.New("metadata superblock checksum mismatch")
	}
	super.CRC = wantCRC
	return super, nil
}

func saveSuperBlock(fs afero.Fs, metaDir string, txid uint64, logFile string) error {
	if logFile == "" || filepath.Base(logFile) != logFile || !strings.HasSuffix(logFile, ".log") {
		return errors.New("invalid metadata log file")
	}
	super := metaSuperBlock{
		FormatVersion:  metaFormatVersion,
		CheckpointTxID: txid,
		LogFile:        logFile,
	}
	payload, err := json.Marshal(super)
	if err != nil {
		return err
	}
	super.CRC = crc32.ChecksumIEEE(payload)
	payload, err = json.Marshal(super)
	if err != nil {
		return err
	}
	name := "SUPER0"
	if txid%2 == 1 {
		name = "SUPER1"
	}
	return writeFileSync(fs, filepath.Join(metaDir, name), payload, 0o600)
}

func nextMetaLogName(current string) string {
	if current == "" {
		return metaLogFile
	}
	base := strings.TrimSuffix(current, ".log")
	n, err := strconv.Atoi(base)
	if err != nil || n < 1 {
		return metaLogFile
	}
	return fmt.Sprintf("%06d.log", n+1)
}

func trimRecentGCRuns(meta *metadata) {
	if len(meta.GC.Recent) > maxRecentGCRuns {
		meta.GC.Recent = append([]gcRun(nil), meta.GC.Recent[len(meta.GC.Recent)-maxRecentGCRuns:]...)
	}
}

func compactMetadata(meta *metadata) {
	ensureMetaMaps(meta)
	for parentID := range meta.DirEntries {
		inode := meta.Inodes[parentID]
		if inode == nil || inode.State != fileStateActive || inode.Kind != fileKindDir {
			delete(meta.DirEntries, parentID)
		}
	}

	referencedInodes := map[uint64]bool{}
	for _, rootID := range meta.Tenants {
		if rootID != 0 {
			referencedInodes[rootID] = true
		}
	}
	for _, entries := range meta.DirEntries {
		for _, childID := range entries {
			if childID != 0 {
				referencedInodes[childID] = true
			}
		}
	}
	for id, inode := range meta.Inodes {
		if inode == nil || (inode.State == fileStateDeleted && !referencedInodes[id]) {
			delete(meta.Inodes, id)
		}
	}

	activeManifestRefs := map[string]bool{}
	for _, inode := range meta.Inodes {
		if inode != nil && inode.State == fileStateActive && inode.Kind == fileKindFile && inode.ManifestID != "" {
			activeManifestRefs[inode.ManifestID] = true
		}
	}
	for id, manifest := range meta.Manifests {
		if manifest == nil || (manifest.State == manifestStateDeleted && manifest.RefCount <= 0 && !activeManifestRefs[id]) {
			delete(meta.Manifests, id)
		}
	}

	for id, chunk := range meta.Chunks {
		if chunk == nil {
			delete(meta.Chunks, id)
			continue
		}
		if chunk.State == chunkStateDeleted && chunk.RefCount <= 0 {
			seg := meta.Segments[chunk.SegmentID]
			if seg == nil || seg.State == segmentStateDeleted {
				delete(meta.Chunks, id)
			}
		}
	}
	segmentRefs := map[string]bool{}
	for _, chunk := range meta.Chunks {
		if chunk != nil && chunk.SegmentID != "" {
			segmentRefs[chunk.SegmentID] = true
		}
	}
	for id, seg := range meta.Segments {
		if seg == nil || (seg.State == segmentStateDeleted && !segmentRefs[id]) {
			delete(meta.Segments, id)
		}
	}
	trimRecentGCRuns(meta)
}

func writeFileSync(fs afero.Fs, path string, data []byte, perm os.FileMode) error {
	if err := fs.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func writeFileAtomicSync(fs afero.Fs, path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for i := 0; i < 100; i++ {
		tempName := filepath.Join(dir, ".tmp-"+filepath.Base(path)+"-"+time.Now().Format("20060102150405.000000000")+"-"+strconv.Itoa(i))
		file, err := fs.OpenFile(tempName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if _, err = file.Write(data); err != nil {
			_ = file.Close()
			_ = fs.Remove(tempName)
			return err
		}
		if err = file.Sync(); err != nil {
			_ = file.Close()
			_ = fs.Remove(tempName)
			return err
		}
		if err = file.Close(); err != nil {
			_ = fs.Remove(tempName)
			return err
		}
		if err = fs.Rename(tempName, path); err != nil {
			_ = fs.Remove(tempName)
			return err
		}
		return nil
	}
	return errors.New("create atomic metadata temp: exhausted attempts")
}

func nowUnix() int64 {
	return time.Now().UnixNano()
}
