package folio

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// OpenDatabase opens or creates a SQLite database at the provided path and
// ensures the schema is available.
func OpenDatabase(ctx context.Context, path string) (*sql.DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	dsn := ensureSQLiteOptions(path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

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
CREATE TABLE IF NOT EXISTS file_metadata (
    file_path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    chunk_overlap INTEGER NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`

	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}

func ensureSQLiteOptions(path string) string {
	dsn := path
	dsn = appendDSNParam(dsn, "_busy_timeout", "8000")
	dsn = appendDSNParam(dsn, "_journal_mode", "WAL")
	return dsn
}

func appendDSNParam(dsn, key, value string) string {
	if strings.Contains(dsn, key+"=") {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + key + "=" + value
}
