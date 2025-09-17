package folio

import (
	"context"
	"fmt"
)

// StoredChunk represents a chunk stored in the database with metadata useful for debugging.
type StoredChunk struct {
	StartLine   int
	EndLine     int
	Content     string
	ContentHash string
	UpdatedAt   string
}

// StoredFileSummary represents a file tracked in the database and the time the newest chunk was updated.
type StoredFileSummary struct {
	FilePath  string
	UpdatedAt string
	Chunks    int
}

// StoredChunks returns all chunks for the provided file path ordered by their starting line.
func (f *Folio) StoredChunks(ctx context.Context, filePath string) ([]StoredChunk, error) {
	rows, err := f.db.QueryContext(ctx, `
SELECT start_line, end_line, content, content_hash, updated_at
FROM chunks
WHERE file_path = ?
ORDER BY start_line
`, filePath)
	if err != nil {
		return nil, fmt.Errorf("query stored chunks: %w", err)
	}
	defer rows.Close()

	var chunks []StoredChunk
	for rows.Next() {
		var chunk StoredChunk
		if err := rows.Scan(&chunk.StartLine, &chunk.EndLine, &chunk.Content, &chunk.ContentHash, &chunk.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan stored chunk: %w", err)
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stored chunks: %w", err)
	}

	return chunks, nil
}

// StoredFiles returns every file path tracked in the database with its most recent update time.
func (f *Folio) StoredFiles(ctx context.Context) ([]StoredFileSummary, error) {
	rows, err := f.db.QueryContext(ctx, `
SELECT file_path, MAX(updated_at) as latest_updated, COUNT(*) as chunk_count
FROM chunks
GROUP BY file_path
ORDER BY file_path
`)
	if err != nil {
		return nil, fmt.Errorf("query stored files: %w", err)
	}
	defer rows.Close()

	var files []StoredFileSummary
	for rows.Next() {
		var summary StoredFileSummary
		if err := rows.Scan(&summary.FilePath, &summary.UpdatedAt, &summary.Chunks); err != nil {
			return nil, fmt.Errorf("scan stored file: %w", err)
		}
		files = append(files, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stored files: %w", err)
	}

	return files, nil
}
