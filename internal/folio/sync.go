package folio

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

type chunkSyncStats struct {
	inserted int
	updated  int
	deleted  int
}

type chunkPosition struct {
	start int
	end   int
}

type fileMetadata struct {
	size    int64
	mtimeNS int64
}

// SyncSummary captures aggregate details about a synchronization run.
type SyncSummary struct {
	FilesProcessed int
	FilesRemoved   int
	ChunksInserted int
	ChunksUpdated  int
	ChunksDeleted  int
	TotalFiles     int
	TotalChunks    int
}

// Synchronize scans the root directory, chunks eligible files, and reconciles
// the results with the SQLite database.
func (f *Folio) Synchronize(ctx context.Context) (SyncSummary, error) {
	logger := f.loggerOrDefault()

	files, err := CollectFiles(f.fs, f.root, f.opts.Extensions, f.opts.IgnoreDirs)
	if err != nil {
		return SyncSummary{}, fmt.Errorf("collect files: %w", err)
	}
	logger.Info("Collected files to process", "count", len(files))

	existingFiles, err := f.loadExistingFiles(ctx)
	if err != nil {
		return SyncSummary{}, err
	}

	summary := SyncSummary{}
	summary.FilesProcessed = len(files)

	var changes ChunkChangeSet

	var existingMu sync.Mutex
	var inserted atomic.Int64
	var updated atomic.Int64
	var deleted atomic.Int64

	workerCount := runtime.NumCPU()
	if workerCount < 1 {
		workerCount = 1
	}

	runPaths := func(paths []string, handler func(string) error) error {
		if len(paths) == 0 {
			return nil
		}

		sem := make(chan struct{}, workerCount)
		var wg sync.WaitGroup
		var firstErr error
		var errMu sync.Mutex

		setFirstErr := func(err error) {
			if err == nil {
				return
			}
			errMu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			errMu.Unlock()
		}

		for _, relPath := range paths {
			errMu.Lock()
			if firstErr != nil {
				errMu.Unlock()
				break
			}
			errMu.Unlock()

			if ctxErr := ctx.Err(); ctxErr != nil {
				setFirstErr(ctxErr)
				break
			}

			sem <- struct{}{}
			wg.Add(1)
			go func(rel string) {
				defer wg.Done()
				defer func() { <-sem }()

				if err := handler(rel); err != nil {
					setFirstErr(err)
					return
				}
			}(relPath)
		}

		wg.Wait()

		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}

	if err := runPaths(files, func(rel string) error {
		stats, delta, syncErr := f.SyncPath(ctx, rel)
		if syncErr != nil {
			return fmt.Errorf("sync file %s: %w", rel, syncErr)
		}

		inserted.Add(int64(stats.inserted))
		updated.Add(int64(stats.updated))
		deleted.Add(int64(stats.deleted))

		existingMu.Lock()
		delete(existingFiles, rel)
		existingMu.Unlock()

		changes.Merge(delta)
		return nil
	}); err != nil {
		return SyncSummary{}, err
	}

	existingMu.Lock()
	remaining := make([]string, 0, len(existingFiles))
	for rel := range existingFiles {
		remaining = append(remaining, rel)
	}
	existingMu.Unlock()
	summary.FilesRemoved = len(remaining)

	if err := runPaths(remaining, func(rel string) error {
		stats, delta, syncErr := f.SyncPath(ctx, rel)
		if syncErr != nil {
			return fmt.Errorf("sync file %s: %w", rel, syncErr)
		}
		inserted.Add(int64(stats.inserted))
		updated.Add(int64(stats.updated))
		deleted.Add(int64(stats.deleted))
		changes.Merge(delta)
		return nil
	}); err != nil {
		return SyncSummary{}, err
	}

	summary.ChunksInserted = int(inserted.Load())
	summary.ChunksUpdated = int(updated.Load())
	summary.ChunksDeleted = int(deleted.Load())

	logger.Info("Synchronization complete", "processed_files", len(files), "removed_files", summary.FilesRemoved, "chunks_inserted", summary.ChunksInserted, "chunks_updated", summary.ChunksUpdated, "chunks_deleted", summary.ChunksDeleted)

	filesCount, chunksCount, err := f.countStoredStats(ctx)
	if err != nil {
		return SyncSummary{}, err
	}
	summary.TotalFiles = filesCount
	summary.TotalChunks = chunksCount

	if err := f.dispatchChunkChanges(ctx, changes); err != nil {
		return SyncSummary{}, err
	}

	return summary, nil
}

func (f *Folio) loadExistingFiles(ctx context.Context) (map[string]struct{}, error) {
	rows, err := f.db.QueryContext(ctx, `SELECT DISTINCT file_path FROM chunks`)
	if err != nil {
		return nil, fmt.Errorf("load existing files: %w", err)
	}
	defer rows.Close()

	existing := make(map[string]struct{})
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("scan existing file: %w", err)
		}
		existing[path] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing files: %w", err)
	}

	return existing, nil
}

func (f *Folio) countStoredStats(ctx context.Context) (int, int, error) {
	var fileCount, chunkCount int
	if err := f.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT file_path) FROM chunks`).Scan(&fileCount); err != nil {
		return 0, 0, fmt.Errorf("count files: %w", err)
	}
	if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks`).Scan(&chunkCount); err != nil {
		return 0, 0, fmt.Errorf("count chunks: %w", err)
	}
	return fileCount, chunkCount, nil
}

func (f *Folio) syncFileChunks(ctx context.Context, tx *sql.Tx, filePath string, chunks []Chunk, changes *ChunkChangeSet) (chunkSyncStats, error) {
	existing, err := loadExistingChunks(ctx, tx, filePath)
	if err != nil {
		return chunkSyncStats{}, err
	}

	seen := make(map[chunkPosition]struct{}, len(chunks))
	var stats chunkSyncStats

	for _, chunk := range chunks {
		pos := chunkPosition{start: chunk.StartLine, end: chunk.EndLine}
		seen[pos] = struct{}{}

		if hash, ok := existing[pos]; ok {
			if hash == chunk.ContentHash {
				continue
			}
			if _, execErr := tx.ExecContext(ctx, `
UPDATE chunks
SET content = ?, content_hash = ?, updated_at = CURRENT_TIMESTAMP
WHERE file_path = ? AND start_line = ? AND end_line = ?
`, chunk.Content, chunk.ContentHash, chunk.FilePath, chunk.StartLine, chunk.EndLine); execErr != nil {
				return chunkSyncStats{}, fmt.Errorf("update chunk: %w", execErr)
			}
			stats.updated++
			if changes != nil {
				changes.Upserts = append(changes.Upserts, chunk)
			}
			continue
		}

		if _, execErr := tx.ExecContext(ctx, `
INSERT INTO chunks (file_path, start_line, end_line, content, content_hash)
VALUES (?, ?, ?, ?, ?)
`, chunk.FilePath, chunk.StartLine, chunk.EndLine, chunk.Content, chunk.ContentHash); execErr != nil {
			return chunkSyncStats{}, fmt.Errorf("insert chunk: %w", execErr)
		}
		stats.inserted++
		if changes != nil {
			changes.Upserts = append(changes.Upserts, chunk)
		}
	}

	for pos := range existing {
		if _, ok := seen[pos]; ok {
			continue
		}
		if _, execErr := tx.ExecContext(ctx, `DELETE FROM chunks WHERE file_path = ? AND start_line = ? AND end_line = ?`, filePath, pos.start, pos.end); execErr != nil {
			return chunkSyncStats{}, fmt.Errorf("delete stale chunk: %w", execErr)
		}
		stats.deleted++
		if changes != nil {
			changes.Deletions = append(changes.Deletions, ChunkIdentifier{FilePath: filePath, StartLine: pos.start, EndLine: pos.end})
		}
	}

	return stats, nil
}

func (f *Folio) deleteFileRecords(ctx context.Context, tx *sql.Tx, filePath string) (int, []ChunkIdentifier, error) {
	rows, err := tx.QueryContext(ctx, `SELECT start_line, end_line FROM chunks WHERE file_path = ?`, filePath)
	if err != nil {
		return 0, nil, fmt.Errorf("select chunks for %s: %w", filePath, err)
	}
	var ids []ChunkIdentifier
	for rows.Next() {
		var start, end int
		if err := rows.Scan(&start, &end); err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("scan chunk position for %s: %w", filePath, err)
		}
		ids = append(ids, ChunkIdentifier{FilePath: filePath, StartLine: start, EndLine: end})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, fmt.Errorf("iterate chunk positions for %s: %w", filePath, err)
	}
	rows.Close()

	res, err := tx.ExecContext(ctx, `DELETE FROM chunks WHERE file_path = ?`, filePath)
	if err != nil {
		return 0, nil, fmt.Errorf("delete file %s: %w", filePath, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, nil, fmt.Errorf("rows affected for %s: %w", filePath, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_metadata WHERE file_path = ?`, filePath); err != nil {
		return 0, nil, fmt.Errorf("delete metadata %s: %w", filePath, err)
	}
	return int(affected), ids, nil
}

func (f *Folio) loadFileMetadata(ctx context.Context, tx *sql.Tx, filePath string) (fileMetadata, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT size, mtime_ns FROM file_metadata WHERE file_path = ?`, filePath)
	var meta fileMetadata
	switch err := row.Scan(&meta.size, &meta.mtimeNS); err {
	case nil:
		return meta, true, nil
	case sql.ErrNoRows:
		return fileMetadata{}, false, nil
	default:
		return fileMetadata{}, false, fmt.Errorf("load file metadata %s: %w", filePath, err)
	}
}

func (f *Folio) upsertFileMetadata(ctx context.Context, tx *sql.Tx, filePath string, meta fileMetadata) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO file_metadata (file_path, size, mtime_ns, updated_at)
VALUES (?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(file_path) DO UPDATE
SET size = excluded.size,
    mtime_ns = excluded.mtime_ns,
    updated_at = CURRENT_TIMESTAMP
`, filePath, meta.size, meta.mtimeNS); err != nil {
		return fmt.Errorf("upsert file metadata %s: %w", filePath, err)
	}
	return nil
}

func loadExistingChunks(ctx context.Context, tx *sql.Tx, filePath string) (map[chunkPosition]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT start_line, end_line, content_hash
FROM chunks
WHERE file_path = ?
`, filePath)
	if err != nil {
		return nil, fmt.Errorf("load existing chunks: %w", err)
	}
	defer rows.Close()

	existing := make(map[chunkPosition]string)
	for rows.Next() {
		var start, end int
		var hash string
		if err := rows.Scan(&start, &end, &hash); err != nil {
			return nil, fmt.Errorf("scan existing chunk: %w", err)
		}
		existing[chunkPosition{start: start, end: end}] = hash
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing chunks: %w", err)
	}

	return existing, nil
}

func (f *Folio) listFilesInDirectory(ctx context.Context, relDir string) ([]string, error) {
	if relDir == "" || relDir == "." {
		return nil, nil
	}

	pattern := relDir + "/%"

	rows, err := f.db.QueryContext(ctx, `SELECT DISTINCT file_path FROM chunks WHERE file_path LIKE ?`, pattern)
	if err != nil {
		return nil, fmt.Errorf("list directory %s: %w", relDir, err)
	}
	defer rows.Close()

	var files []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("scan directory file: %w", err)
		}
		files = append(files, path)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate directory files: %w", err)
	}

	sort.Strings(files)
	return files, nil
}
