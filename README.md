# BlobFS

> A thread-safe, deduplicating object storage system for Go.

BlobFS is a robust Go library designed for managing file storage with built-in deduplication and concurrency safety. It handles file metadata, storage, and retrieval efficiently, making it suitable for applications requiring reliable local object storage.

## Features

*   **Content-Addressable Storage (CAS):** Files are stored based on their content hash (SHA-256), enabling automatic deduplication. Identical files stored at different paths consume storage space only once.
*   **Concurrency Safe:** Built-in locking mechanisms (Read/Write locks) ensure safe concurrent access for reading, writing, and deleting operations.
*   **Metadata Management:** Maintains metadata (creation time, custom options) for each stored object separate from the blob data.
*   **Thread-Safe Garbage Collection:** Includes a garbage collector (`BlobGC`) to safely remove unreferenced blobs without interrupting ongoing read operations.
*   **Atomic Operations:** Ensures critical operations like file creation and deletion are atomic to prevent data corruption.

## Installation

```bash
go get gopkg.d7z.net/blobfs
```

## Usage

### Initialization

```go
package main

import (
	"log"
	"gopkg.d7z.net/blobfs"
)

func main() {
	// Initialize BlobFS with a base directory
	fs, err := blobfs.BlobFS("/path/to/storage")
	if err != nil {
		log.Fatal(err)
	}
    // ...
}
```

### Storing a File (Push)

```go
	file, _ := os.Open("example.txt")
	defer file.Close()

	// Push the file to BlobFS at "docs/example.txt"
	err = fs.Push("docs/example.txt", file, map[string]string{
		"author": "dragon",
	})
	if err != nil {
		log.Fatal(err)
	}
```

### Retrieving a File (Pull)

```go
	// Pull the file content
	content, err := fs.Pull("docs/example.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer content.Close()

	// Access metadata
	log.Printf("Created at: %v", content.CreateAt)
    log.Printf("Options: %v", content.Options)

	// Read data
	// io.Copy(os.Stdout, content)
```

### Garbage Collection

```go
	// Clean up unreferenced blobs
	if err := fs.BlobGC(); err != nil {
		log.Printf("GC failed: %v", err)
	}
```

## Architecture

*   **Blob Storage:** Raw file data is stored in a content-addressable `blob` directory.
*   **Metadata:** File metadata (mapping path to blob hash) is stored in a `meta` directory.
*   **Locking:** A fine-grained `rwLockGroup` manages access to individual files, while a global `gcLocker` coordinates garbage collection and blob deletion.

## License

This project is licensed under the [Apache-2.0](./LICENSE) license.
