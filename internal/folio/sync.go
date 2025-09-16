package folio

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"log/slog"
)

// Options control the behaviour of the synchronisation routine.
type Options struct {
	Extensions   []string
	ChunkSize    int
	ChunkOverlap int
	Logger       *slog.Logger
}

type chunkPosition struct {
	start int
	end   int
}

type chunkSyncStats struct {
	inserted int
	updated  int
	deleted  int
}

// Synchronize scans the root directory, chunks eligible files, and reconciles
// the results with the SQLite database.
func Synchronize(ctx context.Context, db *sql.DB, root string, opts Options) error {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	logger.Info("Starting synchronization", "root", root, "extensions", opts.Extensions, "chunk_size", opts.ChunkSize, "chunk_overlap", opts.ChunkOverlap)

	files, err := CollectFiles(root, opts.Extensions)
	if err != nil {
		return fmt.Errorf("collect files: %w", err)
	}
	logger.Info("Collected files to process", "count", len(files))

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	existingFiles, err := loadExistingFiles(ctx, tx)
	if err != nil {
		return err
	}

	chunkOpts := ChunkOptions{ChunkSize: opts.ChunkSize, ChunkOverlap: opts.ChunkOverlap}

	for _, relPath := range files {
		absPath := filepath.Join(root, filepath.FromSlash(relPath))
		chunks, chunkErr := ChunkFile(absPath, relPath, chunkOpts)
		if chunkErr != nil {
			err = fmt.Errorf("chunk file %s: %w", relPath, chunkErr)
			return err
		}

		stats, syncErr := syncFileChunks(ctx, tx, relPath, chunks)
		if syncErr != nil {
			err = fmt.Errorf("sync file %s: %w", relPath, syncErr)
			return err
		}

		logger.Info("Processed file", "file_path", relPath, "chunks", len(chunks), "inserted", stats.inserted, "updated", stats.updated, "deleted", stats.deleted)

		delete(existingFiles, relPath)
	}

	for relPath := range existingFiles {
		if _, execErr := tx.ExecContext(ctx, `DELETE FROM chunks WHERE file_path = ?`, relPath); execErr != nil {
			err = fmt.Errorf("delete removed file entries for %s: %w", relPath, execErr)
			return err
		}
		logger.Info("Removed chunks for missing file", "file_path", relPath)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("commit transaction: %w", commitErr)
	}

	logger.Info("Synchronization complete", "processed_files", len(files), "removed_files", len(existingFiles))

	return nil
}

func loadExistingFiles(ctx context.Context, tx *sql.Tx) (map[string]struct{}, error) {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT file_path FROM chunks`)
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

func syncFileChunks(ctx context.Context, tx *sql.Tx, filePath string, chunks []Chunk) (chunkSyncStats, error) {
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
