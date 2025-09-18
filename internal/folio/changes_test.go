package folio

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type recordingTarget struct {
	calls []ChunkChangeSet
}

func (r *recordingTarget) ApplyChunkChanges(ctx context.Context, changes ChunkChangeSet) error {
	if changes.IsEmpty() {
		return nil
	}
	r.calls = append(r.calls, changes)
	return nil
}

func (r *recordingTarget) reset() {
	r.calls = nil
}

func TestSynchronizeDispatchesChunkChanges(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := EnsureSchema(ctx, db); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	fs := newMapFileSystem(map[string]string{
		"project/main.txt": "line1\nline2\nline3\nline4",
	})

	opts := Options{
		Extensions:   []string{".txt"},
		ChunkSize:    2,
		ChunkOverlap: 0,
	}

	folio := NewFolio(db, ".", opts, nil)
	folio.SetFileSystem(fs)

	target := &recordingTarget{}
	folio.RegisterSyncTarget(target)

	if _, err := folio.Synchronize(ctx); err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}

	if len(target.calls) != 1 {
		t.Fatalf("expected 1 change set, got %d", len(target.calls))
	}
	first := target.calls[0]
	if len(first.Upserts) == 0 {
		t.Fatalf("expected upserts in first change set")
	}
	if len(first.Deletions) != 0 {
		t.Fatalf("expected no deletions initially, got %d", len(first.Deletions))
	}

	target.reset()

	delete(fs.fs, "project/main.txt")

	if _, err := folio.Synchronize(ctx); err != nil {
		t.Fatalf("removal sync failed: %v", err)
	}

	if len(target.calls) != 1 {
		t.Fatalf("expected 1 change set for removal, got %d", len(target.calls))
	}
	removal := target.calls[0]
	if len(removal.Deletions) == 0 {
		t.Fatalf("expected deletions when file removed")
	}
	if len(removal.Upserts) != 0 {
		t.Fatalf("expected no upserts on removal, got %d", len(removal.Upserts))
	}
}
