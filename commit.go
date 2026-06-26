package blobfs

import "fmt"

func addManifestRefDelta(manifest *manifestRecord, delta int, manifestDeltas, chunkDeltas map[string]int) {
	if manifest == nil || delta == 0 {
		return
	}
	manifestDeltas[manifest.ManifestID] += delta
	seen := map[string]bool{}
	for _, ref := range manifest.Chunks {
		if seen[ref.ChunkID] {
			continue
		}
		seen[ref.ChunkID] = true
		chunkDeltas[ref.ChunkID] += delta
	}
}

func appendRefDeltaOpsLocked(meta *metadata, ops *[]metaOp, manifestRecords map[string]*manifestRecord, manifestDeltas, chunkDeltas map[string]int, now int64, refCountWarnings *[]string) {
	for manifestID, delta := range manifestDeltas {
		manifest := manifestRecords[manifestID]
		if manifest == nil {
			manifest = meta.Manifests[manifestID]
		}
		if manifest == nil {
			continue
		}
		next := cloneManifest(manifest)
		next.RefCount += delta
		if next.RefCount < 0 {
			next.RefCount = 0
		}
		if next.RefCount == 0 {
			next.State = manifestStateDeleted
			next.DeletedAt = now
		} else {
			if next.State != manifestStateDegraded {
				next.State = manifestStateActive
			}
			next.DeletedAt = 0
			next.LastLiveAt = now
		}
		*ops = append(*ops, metaOp{Type: "put_manifest", Manifest: next})
	}
	pendingChunks := map[string]*chunkRecord{}
	for i := range *ops {
		op := &(*ops)[i]
		if op.Type == "put_chunk" && op.Chunk != nil {
			pendingChunks[op.Chunk.ChunkID] = op.Chunk
		}
	}
	for chunkID, delta := range chunkDeltas {
		chunk := pendingChunks[chunkID]
		if chunk == nil {
			chunk = meta.Chunks[chunkID]
			if chunk == nil {
				continue
			}
		}
		next := *chunk
		next.RefCount += delta
		if next.RefCount < 0 {
			if refCountWarnings != nil {
				*refCountWarnings = append(*refCountWarnings, fmt.Sprintf(
					"chunk %s refcount went negative (delta=%d, was=%d)",
					chunkID, delta, chunk.RefCount))
			}
			next.RefCount = 0
		}
		if next.RefCount > 0 {
			if next.State != chunkStateMissing {
				next.State = chunkStateActive
			}
			next.LastSeenAt = now
			next.DeletedAt = 0
			next.GarbageCandidateAt = 0
			next.GarbageSeenCount = 0
		}
		*ops = append(*ops, metaOp{Type: "put_chunk", Chunk: &next})
	}
}

func addDeletedManifestOpsLocked(meta *metadata, manifestID string, ops *[]metaOp, now int64, refCountWarnings *[]string) {
	manifest := meta.Manifests[manifestID]
	if manifest == nil {
		return
	}
	manifestDeltas := map[string]int{}
	chunkDeltas := map[string]int{}
	addManifestRefDelta(manifest, -1, manifestDeltas, chunkDeltas)
	appendRefDeltaOpsLocked(meta, ops, map[string]*manifestRecord{manifestID: manifest}, manifestDeltas, chunkDeltas, now, refCountWarnings)
}
