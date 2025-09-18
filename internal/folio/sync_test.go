package folio

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSyncFileChunksInsertUpdateDelete(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer db.Close()

	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatalf("failed to ensure schema: %v", err)
	}

	folio := &Folio{
		db: db,
		fs: OSFileSystem{},
	}

	filePath := "sample.txt"

	chunksInitial := []Chunk{
		makeChunk(filePath, 1, 3, "line1\nline2\nline3"),
		makeChunk(filePath, 4, 6, "line4\nline5\nline6"),
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	stats, err := folio.syncFileChunks(ctx, tx, filePath, chunksInitial, &ChunkChangeSet{})
	if err != nil {
		t.Fatalf("syncFileChunks insert failed: %v", err)
	}
	if stats.inserted != 2 || stats.updated != 0 || stats.deleted != 0 {
		t.Fatalf("unexpected stats after insert: %+v", stats)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	assertChunkCount(t, ctx, db, filePath, 2)

	chunksUpdated := []Chunk{
		makeChunk(filePath, 1, 3, "line1\nline2\nline3"),
		makeChunk(filePath, 4, 6, "line4\nline5\nlineX"),
		makeChunk(filePath, 7, 7, "line7"),
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	stats, err = folio.syncFileChunks(ctx, tx, filePath, chunksUpdated, &ChunkChangeSet{})
	if err != nil {
		t.Fatalf("syncFileChunks update failed: %v", err)
	}
	if stats.inserted != 1 || stats.updated != 1 || stats.deleted != 0 {
		t.Fatalf("unexpected stats after update: %+v", stats)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	assertChunkCount(t, ctx, db, filePath, 3)

	chunksFinal := []Chunk{
		makeChunk(filePath, 1, 3, "line1\nline2\nline3"),
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	stats, err = folio.syncFileChunks(ctx, tx, filePath, chunksFinal, &ChunkChangeSet{})
	if err != nil {
		t.Fatalf("syncFileChunks delete failed: %v", err)
	}
	if stats.inserted != 0 || stats.updated != 0 || stats.deleted != 2 {
		t.Fatalf("unexpected stats after delete: %+v", stats)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	assertChunkCount(t, ctx, db, filePath, 1)
}

func makeChunk(filePath string, start, end int, content string) Chunk {
	sum := md5.Sum([]byte(content))
	return Chunk{
		FilePath:    filePath,
		StartLine:   start,
		EndLine:     end,
		Content:     content,
		ContentHash: hex.EncodeToString(sum[:]),
	}
}

func assertChunkCount(t *testing.T, ctx context.Context, db *sql.DB, filePath string, expected int) {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE file_path = ?`, filePath).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != expected {
		t.Fatalf("expected %d chunks, got %d", expected, count)
	}
}
