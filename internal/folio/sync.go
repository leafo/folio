package folio

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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

// Synchronize scans the root directory, chunks eligible files, and reconciles
// the results with the SQLite database.
func (f *Folio) Synchronize(ctx context.Context) error {
	logger := f.loggerOrDefault()

	files, err := CollectFiles(f.fs, f.root, f.opts.Extensions)
	if err != nil {
		return fmt.Errorf("collect files: %w", err)
	}
	logger.Info("Collected files to process", "count", len(files))

	existingFiles, err := f.loadExistingFiles(ctx)
	if err != nil {
		return err
	}

	totals := chunkSyncStats{}

	for _, relPath := range files {
		stats, syncErr := f.SyncPath(ctx, relPath)
		if syncErr != nil {
			return fmt.Errorf("sync file %s: %w", relPath, syncErr)
		}
		totals.inserted += stats.inserted
		totals.updated += stats.updated
		totals.deleted += stats.deleted

		delete(existingFiles, relPath)
	}

	removedCount := len(existingFiles)
	for relPath := range existingFiles {
		stats, syncErr := f.SyncPath(ctx, relPath)
		if syncErr != nil {
			return fmt.Errorf("sync file %s: %w", relPath, syncErr)
		}
		totals.deleted += stats.deleted
		delete(existingFiles, relPath)
	}

	logger.Info("Synchronization complete", "processed_files", len(files), "removed_files", removedCount, "chunks_inserted", totals.inserted, "chunks_updated", totals.updated, "chunks_deleted", totals.deleted)

	return nil
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
