# BlobFS

[![Go Reference](https://pkg.go.dev/badge/gopkg.d7z.net/blobfs.svg)](https://pkg.go.dev/gopkg.d7z.net/blobfs)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)

BlobFS is a local content-addressed storage library for Go. It stores file data as deduplicated chunks in append-only segment files, keeps metadata in a crash-recoverable transaction log, and exposes both object-style APIs and an `afero.Fs` virtual filesystem.

## Features

- Content-addressed chunks with SHA-256 verification on reads.
- FastCDC-style streaming chunking for large files.
- Tenant-scoped or global deduplication.
- Append-only segment storage with zstd compression and CRC32C records.
- Metadata transaction log, checkpoints, and explicit recovery APIs.
- Tombstone deletes, mark/sweep GC, and segment compaction.
- Range reads, metadata-only updates, and explicit directory records.
- `afero.Fs` and tenant-rooted `io/fs` support.

## Install

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

	store, err := blobfs.Open("./blobfs-data", blobfs.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
		log.Fatal(err)
	}

	_, err = store.Put(ctx, "tenant-a", "docs/hello.txt", bytes.NewReader([]byte("hello blobfs")), nil)
	if err != nil {
		log.Fatal(err)
	}

	r, err := store.OpenObject(ctx, "tenant-a", "docs/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", data)
}
```

## API Overview

```go
store, err := blobfs.Open(path, blobfs.DefaultConfig())
store, err := blobfs.OpenFS(fs, path, blobfs.DefaultConfig())

put, err := store.Put(ctx, tenantID, path, reader, metadata)
info, err := store.StatObject(ctx, tenantID, path)
reader, err := store.OpenObject(ctx, tenantID, path)
rangeReader, err := store.OpenRange(ctx, tenantID, path, offset, length)
info, err = store.UpdateMetadata(ctx, tenantID, path, metadata)
err = store.DeleteObject(ctx, tenantID, path)

health, err := store.Health(ctx)
stats, err := store.Stats(ctx)
diagnose, err := store.Diagnose(ctx, blobfs.DiagnoseOptions{})
repair, err := store.Repair(ctx, blobfs.RepairOptions{DryRun: true})
```

`Store` implements `afero.Fs`, so existing afero helpers can use tenant-prefixed paths such as `tenant-a/docs/file.txt`. `TenantFS(tenantID)` exposes a read-only `io/fs` view rooted at one tenant.

## Documentation

See [DOCS.md](./DOCS.md) for storage layout, metadata design, GC, recovery, VFS semantics, and configuration details.

## Test

```sh
make lint
make test
make race
make cover
```

## License

BlobFS is licensed under the [Apache License 2.0](./LICENSE).
