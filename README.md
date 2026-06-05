# BlobFS

[![Go Reference](https://pkg.go.dev/badge/gopkg.d7z.net/blobfs.svg)](https://pkg.go.dev/gopkg.d7z.net/blobfs)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)

BlobFS is a local content-addressed file storage library for Go. It stores files as manifests of chunks, appends chunk payloads to segment files, supports metadata-only deletes, and reclaims space with asynchronous mark/sweep GC and segment compaction.

`*Store` also implements [`afero.Fs`](https://github.com/spf13/afero), so it can be used directly as a virtual filesystem. `TenantFS` exposes a standard `io/fs` view rooted at one tenant.

## Features

- SHA-256 content addressing and deduplication.
- Streaming FastCDC-style content-defined chunking.
- Append-only segment files with fixed two-level fanout.
- Tombstone deletes with asynchronous GC and compaction.
- Object range reads.
- Metadata-only updates.
- Metadata-only large directory `Rename` and `RemoveAll`; child subtrees are not synchronously rewritten.
- Object and store integrity checks.
- Explicit directory records with directory indexes.
- Generation-checked VFS writes to avoid silent lost updates.
- Context-aware VFS commits through `OpenFileContext`.
- `afero.Fs` and tenant-rooted `io/fs` support.
- Regular file modes clear executable bits by default.

## Installation

```sh
go get gopkg.d7z.net/blobfs
```

BlobFS requires Go 1.23 or newer.

## Quick Start

```go
package main

import (
	"bytes"
	"context"
	"io"
	"log"

	"gopkg.d7z.net/blobfs"
)

func main() {
	ctx := context.Background()

	store, err := blobfs.Open("./data/blobfs", blobfs.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		log.Fatal(err)
	}

	_, err = store.Put(ctx, "tenant-a", "docs/hello.txt", bytes.NewReader([]byte("hello blobfs")), map[string]string{
		"content-type": "text/plain",
	})
	if err != nil {
		log.Fatal(err)
	}

	reader, err := store.OpenObject(ctx, "tenant-a", "docs/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", data)
}
```

## Object API

```go
result, err := store.Put(ctx, tenantID, path, reader, metadata)
info, err := store.StatObject(ctx, tenantID, path)
reader, err := store.OpenObject(ctx, tenantID, path)
rangeReader, err := store.OpenRange(ctx, tenantID, path, offset, length)
info, err = store.UpdateMetadata(ctx, tenantID, path, metadata)
err = store.DeleteObject(ctx, tenantID, path)
```

Object metadata is a `map[string]string`. `UpdateMetadata` replaces metadata without rewriting content or changing the manifest.

Nested object paths require explicit parent directories. Create them through the VFS API before calling `Put`.

## Afero Filesystem

`Store` implements `afero.Fs`. VFS paths use `tenant/path` form.

```go
fs, err := blobfs.OpenFS(afero.NewMemMapFs(), "/blobfs", blobfs.DefaultConfig())
if err != nil {
	log.Fatal(err)
}
defer fs.Close()

if err := fs.MkdirAll("tenant-a/docs", 0o755); err != nil {
	log.Fatal(err)
}
if err := afero.WriteFile(fs, "tenant-a/docs/file.txt", []byte("hello"), 0o644); err != nil {
	log.Fatal(err)
}
```

Directory records, mode, atime, mtime, uid, and gid are stored in BlobFS metadata.

Directories are explicit; BlobFS does not synthesize missing parents from object names. Writable VFS handles commit through temporary write sessions, enforces `MaxOpenWriteSessions`, and fails with `ErrConflict` if the file generation changed while the handle was open.

Use `OpenFileContext` when a writable VFS handle needs caller-controlled cancellation for later `Sync` or `Close` commits. Plain `OpenFile` uses the store lifecycle context.

## Standard Library FS

Use `TenantFS` when a consumer expects `io/fs.FS`:

```go
tenant := store.TenantFS("tenant-a")
data, err := fs.ReadFile(tenant, "docs/file.txt")
```

## Integrity and GC

```go
check, err := store.CheckObject(ctx, tenantID, path)
scrub, err := store.Scrub(ctx, blobfs.ScrubOptions{CheckFiles: true})
gc, err := store.RunGC(ctx, blobfs.GCOptions{Compact: true})
health, err := store.Health(ctx)
stats, err := store.Stats(ctx)
diagnose, err := store.Diagnose(ctx, blobfs.DiagnoseOptions{CheckFiles: true})
repair, err := store.Repair(ctx, blobfs.RepairOptions{DryRun: true, CleanStaging: true})
err = blobfs.RemoveStaleLock("./data/blobfs") // only after confirming no live process owns the store
```

Reads verify segment records, payload checksums, decompressed sizes, and chunk hashes before returning bytes. Integrity checks additionally verify manifest references, chunk metadata, and file hashes. Corrupt chunks and segments are marked `CORRUPT` and are not reused for reads or deduplication.

`Health` and `Stats` are lightweight metadata-oriented APIs. `Health` also reports checkpoint/log health; `Stats` keeps total GC runs plus a bounded recent history internally. `Diagnose` can optionally scan segment and staging directories. `Repair` only performs low-risk actions selected by `RepairOptions`; it defaults to dry-run and executes only when `Apply` is true. `RemoveStaleLock` is a standalone crash-recovery helper and must only be called after external ownership checks.

## Configuration

```go
cfg := blobfs.DefaultConfig()
cfg.SegmentSize = 256 << 20
cfg.MaxFileSize = 1 << 40
cfg.GC.CompactGarbageRatio = 0.6

store, err := blobfs.Open("./data/blobfs", cfg)
```

Defaults:

- `SegmentSize`: 256 MiB
- `MaxFileSize`: 1 TiB
- `MaxTenantLength`: 128 bytes
- `MaxPathLength`: 4096 bytes
- `MaxComponentLength`: 255 bytes
- `MaxOpenWriteSessions`: 1024
- `Compression`: zstd
- `Checksum`: CRC32C
- `DedupScope`: tenant
- `Chunking`: FastCDC
- `GC.SafetyWindow`: 24h
- `GC.CandidateConfirmCycles`: 2
- `GC.SegmentDeleteDelay`: 24h
- `GC.CompactGarbageRatio`: 0.6

## Storage Layout

```text
base/
  meta/
    LOCK
    SUPER0
    SUPER1
    checkpoint.json
    txlog/
      000001.log
      000002.log
  data/
    segments/
      0000/
        0000/
          0000000000000001.blob
          0000000000000002.blob
    staging/
```

Metadata is persisted as typed inode/dentry transactions in an append-only metadata log. `SUPER0`/`SUPER1` select the active log generation. Periodic checkpoints write a compacted metadata snapshot, switch to a new empty log, and prune obsolete deleted metadata so startup replay and checkpoint size stay bounded. Segment files are numeric `.blob` files and object data is staged before metadata makes it visible.

## Testing

```sh
make lint
make test
make race
make cover
```

## Documentation

See [DOCS.md](./DOCS.md) for the core design.

## License

BlobFS is licensed under the [Apache License 2.0](./LICENSE).
