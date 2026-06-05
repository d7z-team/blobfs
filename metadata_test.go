package blobfs

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
)

func TestLoadMetadataIgnoresTrailingPartialFrame(t *testing.T) {
	fsys := afero.NewMemMapFs()
	metaDir := "/meta"
	logPath := filepath.Join(metaDir, "txlog", metaLogFile)
	if err := fsys.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		t.Fatalf("mkdir txlog: %v", err)
	}
	log, err := fsys.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open txlog: %v", err)
	}
	now := nowUnix()
	root := &inodeRecord{
		InodeID:   1,
		TenantID:  "tenant-a",
		Kind:      fileKindDir,
		Name:      "",
		State:     fileStateActive,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := writeMetaTx(log, metaTx{TxID: 1, Ops: []metaOp{
		{Type: "put_tenant", TenantID: "tenant-a", ChildID: root.InodeID},
		{Type: "put_inode", Inode: root},
	}}); err != nil {
		_ = log.Close()
		t.Fatalf("write tx: %v", err)
	}
	if _, err := log.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		_ = log.Close()
		t.Fatalf("write partial frame: %v", err)
	}
	_ = log.Close()
	meta, logName, report, err := loadMetadata(fsys, metaDir)
	if err != nil {
		t.Fatalf("load metadata with trailing partial frame: %v", err)
	}
	if len(report.ReplayWarnings) != 1 {
		t.Fatalf("partial frame warning count = %d", len(report.ReplayWarnings))
	}
	if logName != metaLogFile {
		t.Fatalf("log name = %q, want %q", logName, metaLogFile)
	}
	if meta.Tenants["tenant-a"] != root.InodeID || meta.TxID != 1 {
		t.Fatalf("metadata was not replayed: %+v", meta)
	}
}

func TestReplayMetaLogRejectsBadChecksum(t *testing.T) {
	fsys := afero.NewMemMapFs()
	logPath := "/meta/txlog/" + metaLogFile
	if err := fsys.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		t.Fatalf("mkdir txlog: %v", err)
	}
	log, err := fsys.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open txlog: %v", err)
	}
	var header [12]byte
	binary.LittleEndian.PutUint32(header[0:4], metaFrameMagic)
	binary.LittleEndian.PutUint32(header[4:8], 1)
	binary.LittleEndian.PutUint32(header[8:12], 0)
	if _, err := log.Write(header[:]); err != nil {
		_ = log.Close()
		t.Fatalf("write header: %v", err)
	}
	if _, err := log.Write([]byte{0xff}); err != nil {
		_ = log.Close()
		t.Fatalf("write payload: %v", err)
	}
	_ = log.Close()
	if _, err := replayMetaLog(fsys, logPath, newMetadata()); err == nil {
		t.Fatal("bad checksum should fail replay")
	}
}

func TestRecoverInProgressMetadataResetsCompactingSegments(t *testing.T) {
	meta := newMetadata()
	meta.Segments["0000000000000001"] = &segmentRecord{SegmentID: "0000000000000001", State: segmentStateCompacting}
	recoverInProgressMetadata(meta)
	if meta.Segments["0000000000000001"].State != segmentStateSealed {
		t.Fatalf("compacting segment was not recovered: %+v", meta.Segments["0000000000000001"])
	}
}

func TestGCHistoryKeepsTotalAndBoundsRecentRuns(t *testing.T) {
	meta := newMetadata()
	total := maxRecentGCRuns + 17
	for i := 1; i <= total; i++ {
		applyMetaTx(meta, metaTx{TxID: uint64(i), Ops: []metaOp{{Type: "append_gcrun", GCRun: &gcRun{Epoch: int64(i), State: "DONE"}}}})
	}
	if meta.GC.TotalRuns != int64(total) {
		t.Fatalf("total runs = %d, want %d", meta.GC.TotalRuns, total)
	}
	if meta.GC.LastEpoch != int64(total) {
		t.Fatalf("last epoch = %d, want %d", meta.GC.LastEpoch, total)
	}
	if len(meta.GC.Recent) != maxRecentGCRuns {
		t.Fatalf("recent runs = %d, want %d", len(meta.GC.Recent), maxRecentGCRuns)
	}
	if meta.GC.Recent[0].Epoch != int64(total-maxRecentGCRuns+1) {
		t.Fatalf("first retained epoch = %d", meta.GC.Recent[0].Epoch)
	}
}
