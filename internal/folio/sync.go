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

	files, err := CollectFiles(f.fs, f.root, f.opts.Extensions)
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

	var existingMu sync.Mutex
	var inserted atomic.Int64
	var updated atomic.Int64
	var deleted atomic.Int64

	workerCount := runtime.NumCPU()
	if workerCount < 1 {
		workerCount = 1
	}

	runBatch := func(paths []string, removeFromExisting bool) error {
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

				stats, syncErr := f.SyncPath(ctx, rel)
				if syncErr != nil {
					setFirstErr(fmt.Errorf("sync file %s: %w", rel, syncErr))
					return
				}

				inserted.Add(int64(stats.inserted))
				updated.Add(int64(stats.updated))
				deleted.Add(int64(stats.deleted))

				if removeFromExisting {
					existingMu.Lock()
					delete(existingFiles, rel)
					existingMu.Unlock()
				}
			}(relPath)
		}

		wg.Wait()

		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}

	if err := runBatch(files, true); err != nil {
		return SyncSummary{}, err
	}

	existingMu.Lock()
	remaining := make([]string, 0, len(existingFiles))
	for rel := range existingFiles {
		remaining = append(remaining, rel)
	}
	existingMu.Unlock()
	summary.FilesRemoved = len(remaining)

	if err := runBatch(remaining, false); err != nil {
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

func (f *Folio) syncFileChunks(ctx context.Context, tx *sql.Tx, filePath string, chunks []Chunk) (chunkSyncStats, error) {
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
			continue
		}

		if _, execErr := tx.ExecContext(ctx, `
INSERT INTO chunks (file_path, start_line, end_line, content, content_hash)
VALUES (?, ?, ?, ?, ?)
`, chunk.FilePath, chunk.StartLine, chunk.EndLine, chunk.Content, chunk.ContentHash); execErr != nil {
			return chunkSyncStats{}, fmt.Errorf("insert chunk: %w", execErr)
		}
		stats.inserted++
	}

	for pos := range existing {
		if _, ok := seen[pos]; ok {
			continue
		}
		if _, execErr := tx.ExecContext(ctx, `DELETE FROM chunks WHERE file_path = ? AND start_line = ? AND end_line = ?`, filePath, pos.start, pos.end); execErr != nil {
			return chunkSyncStats{}, fmt.Errorf("delete stale chunk: %w", execErr)
		}
		stats.deleted++
	}

	return stats, nil
}

func (f *Folio) deleteFileRecords(ctx context.Context, tx *sql.Tx, filePath string) (int, error) {
	res, err := tx.ExecContext(ctx, `DELETE FROM chunks WHERE file_path = ?`, filePath)
	if err != nil {
		return 0, fmt.Errorf("delete file %s: %w", filePath, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected for %s: %w", filePath, err)
	}
	return int(affected), nil
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
