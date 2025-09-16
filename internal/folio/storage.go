package folio

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
)

// OpenDatabase opens or creates a SQLite database at the provided path and
// ensures the schema is available.
func OpenDatabase(ctx context.Context, path string) (*sql.DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	if err := EnsureSchema(ctx, db); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// EnsureSchema creates the required tables if they do not already exist.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS chunks (
    file_path TEXT NOT NULL,
    start_line INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (file_path, start_line, end_line)
);
CREATE INDEX IF NOT EXISTS idx_chunks_file_path ON chunks(file_path);
`

	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}
