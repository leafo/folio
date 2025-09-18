package folio

import "context"

// ChunkIdentifier uniquely identifies a stored chunk by file path and line range.
type ChunkIdentifier struct {
	FilePath  string
	StartLine int
	EndLine   int
}

// ChunkChangeSet aggregates chunk insertions/updates and deletions.
type ChunkChangeSet struct {
	Upserts   []Chunk
	Deletions []ChunkIdentifier
}

// Merge combines another change set into the receiver.
func (c *ChunkChangeSet) Merge(other ChunkChangeSet) {
	if len(other.Upserts) > 0 {
		c.Upserts = append(c.Upserts, other.Upserts...)
	}
	if len(other.Deletions) > 0 {
		c.Deletions = append(c.Deletions, other.Deletions...)
	}
}

// IsEmpty reports whether there are no recorded changes.
func (c ChunkChangeSet) IsEmpty() bool {
	return len(c.Upserts) == 0 && len(c.Deletions) == 0
}

// ChunkSyncTarget consumes chunk change notifications.
type ChunkSyncTarget interface {
	ApplyChunkChanges(ctx context.Context, changes ChunkChangeSet) error
}
