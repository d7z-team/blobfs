// Package blobfs provides a content-addressed blob store with a small
// filesystem-style namespace.
//
// A Store keeps metadata in a write-ahead log and stores file content in
// immutable compressed segments. Objects are addressed by tenant and path, and
// duplicate chunks are shared according to the configured deduplication scope.
//
// The package exposes two API styles. Object APIs such as Put, OpenObject, and
// DeleteObject are explicit and context-aware. The Store also implements
// afero.Fs for callers that prefer filesystem operations over tenant/path
// object calls.
//
// Garbage collection is explicit unless StartBackground is used. Namespace
// operations make paths visible or unreachable immediately, while unreachable
// metadata and physical segment files are reclaimed by RunGC.
//
// A minimal object workflow:
//
//	ctx := context.Background()
//	store, err := blobfs.Open("/var/lib/blobfs", blobfs.DefaultConfig())
//	if err != nil {
//		return err
//	}
//	defer store.Close()
//
//	if err := store.MkdirAll("tenant-a/docs", 0o755); err != nil {
//		return err
//	}
//	if _, err := store.Put(ctx, "tenant-a", "docs/readme.txt", input, nil); err != nil {
//		return err
//	}
//	reader, err := store.OpenObject(ctx, "tenant-a", "docs/readme.txt")
//	if err != nil {
//		return err
//	}
//	defer reader.Close()
package blobfs
