package folio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"log/slog"
)

// Options control chunking and file selection for Folio operations.
type Options struct {
	Extensions   []string
	ChunkSize    int
	ChunkOverlap int
	IgnoreDirs   []string
}

// Folio manages scanning, chunking, and persisting file content metadata.
type Folio struct {
	db     *sql.DB
	root   string
	opts   Options
	logger *slog.Logger
	fs     FileSystem
}

// NewFolio constructs a Folio instance using the provided database connection and configuration.
func NewFolio(db *sql.DB, root string, opts Options, logger *slog.Logger) *Folio {
	if logger == nil {
		logger = slog.Default()
	}

	f := &Folio{
		db:     db,
		root:   root,
		opts:   opts,
		logger: logger,
		fs:     OSFileSystem{},
	}
	return f
}

// SetFileSystem overrides the filesystem implementation used for file access.
func (f *Folio) SetFileSystem(fs FileSystem) {
	if fs == nil {
		f.fs = OSFileSystem{}
		return
	}
	f.fs = fs
}

func (f *Folio) chunkOptions() ChunkOptions {
	return ChunkOptions{ChunkSize: f.opts.ChunkSize, ChunkOverlap: f.opts.ChunkOverlap}
}

func (f *Folio) shouldIndex(relPath string) bool {
	if f.isIgnored(relPath) {
		return false
	}
	if len(f.opts.Extensions) == 0 {
		return true
	}
	ext := strings.ToLower(filepath.Ext(relPath))
	for _, allowed := range f.opts.Extensions {
		if strings.ToLower(strings.TrimSpace(allowed)) == ext {
			return true
		}
	}
	return false
}

func (f *Folio) isIgnored(relPath string) bool {
	if len(f.opts.IgnoreDirs) == 0 {
		return false
	}
	relPath = strings.TrimLeft(filepath.ToSlash(relPath), "/")
	for _, dir := range f.opts.IgnoreDirs {
		d := strings.Trim(strings.TrimSpace(dir), "/")
		if d == "" {
			continue
		}
		if relPath == d || strings.HasPrefix(relPath, d+"/") {
			return true
		}
	}
	return false
}

func (f *Folio) absPath(relPath string) string {
	return filepath.Join(f.root, filepath.FromSlash(relPath))
}

func (f *Folio) relativePath(path string) string {
	rel, err := filepath.Rel(f.root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}

// SyncPath chunks the provided relative file path and reconciles its state in the database.
func (f *Folio) SyncPath(ctx context.Context, relPath string) (chunkSyncStats, error) {
	if !f.shouldIndex(relPath) {
		tx, err := f.db.BeginTx(ctx, nil)
		if err != nil {
			return chunkSyncStats{}, fmt.Errorf("begin transaction: %w", err)
		}
		deleted, err := f.deleteFileRecords(ctx, tx, relPath)
		if err != nil {
			tx.Rollback()
			return chunkSyncStats{}, err
		}
		if err := tx.Commit(); err != nil {
			return chunkSyncStats{}, fmt.Errorf("commit transaction: %w", err)
		}
		if deleted > 0 {
			f.loggerOrDefault().Info("Removed file from index", "file_path", relPath, "deleted", deleted, "reason", "filtered")
		}
		return chunkSyncStats{deleted: deleted}, nil
	}

	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return chunkSyncStats{}, fmt.Errorf("begin transaction: %w", err)
	}
	stats, err := f.syncPathTx(ctx, tx, relPath)
	if err != nil {
		tx.Rollback()
		return chunkSyncStats{}, err
	}
	if err := tx.Commit(); err != nil {
		return chunkSyncStats{}, fmt.Errorf("commit transaction: %w", err)
	}
	return stats, nil
}

func (f *Folio) syncPathTx(ctx context.Context, tx *sql.Tx, relPath string) (chunkSyncStats, error) {
	logger := f.loggerOrDefault()

	chunks, err := f.chunkFile(relPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			deleted, delErr := f.deleteFileRecords(ctx, tx, relPath)
			if delErr != nil {
				return chunkSyncStats{}, delErr
			}
			logger.Info("Removed file from index", "file_path", relPath, "deleted", deleted)
			return chunkSyncStats{deleted: deleted}, nil
		}
		return chunkSyncStats{}, err
	}
	stats, err := f.syncFileChunks(ctx, tx, relPath, chunks)
	if err != nil {
		return chunkSyncStats{}, err
	}
	logger.Info("Synced file", "file_path", relPath, "chunks", len(chunks), "inserted", stats.inserted, "updated", stats.updated, "deleted", stats.deleted)
	return stats, nil
}

func (f *Folio) loggerOrDefault() *slog.Logger {
	if f.logger != nil {
		return f.logger
	}
	return slog.Default()
}
