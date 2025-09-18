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
	Meilisearch  MeilisearchConfig
}

// Folio manages scanning, chunking, and persisting file content metadata.
type Folio struct {
	db      *sql.DB
	root    string
	opts    Options
	logger  *slog.Logger
	fs      FileSystem
	force   bool
	targets []ChunkSyncTarget
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
		force:  false,
	}
	f.initMeilisearch()
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

// SetForceProcessing toggles whether SyncPath should bypass metadata shortcuts.
func (f *Folio) SetForceProcessing(force bool) {
	f.force = force
}

// RegisterSyncTarget registers a sync target to receive chunk change notifications.
func (f *Folio) RegisterSyncTarget(target ChunkSyncTarget) {
	if target == nil {
		return
	}
	f.targets = append(f.targets, target)
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
	return pathMatchesIgnore(relPath, f.opts.IgnoreDirs)
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
func (f *Folio) SyncPath(ctx context.Context, relPath string) (chunkSyncStats, ChunkChangeSet, error) {
	var changes ChunkChangeSet

	if !f.shouldIndex(relPath) {
		tx, err := f.db.BeginTx(ctx, nil)
		if err != nil {
			return chunkSyncStats{}, ChunkChangeSet{}, fmt.Errorf("begin transaction: %w", err)
		}
		deleted, ids, err := f.deleteFileRecords(ctx, tx, relPath)
		if err != nil {
			tx.Rollback()
			return chunkSyncStats{}, ChunkChangeSet{}, err
		}
		if err := tx.Commit(); err != nil {
			return chunkSyncStats{}, ChunkChangeSet{}, fmt.Errorf("commit transaction: %w", err)
		}
		if deleted > 0 {
			f.loggerOrDefault().Info("Removed file from index", "file_path", relPath, "deleted", deleted, "reason", "filtered")
		}
		changes.Deletions = append(changes.Deletions, ids...)
		return chunkSyncStats{deleted: deleted}, changes, nil
	}

	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return chunkSyncStats{}, ChunkChangeSet{}, fmt.Errorf("begin transaction: %w", err)
	}
	stats, delta, err := f.syncPathTx(ctx, tx, relPath)
	if err != nil {
		tx.Rollback()
		return chunkSyncStats{}, ChunkChangeSet{}, err
	}
	if err := tx.Commit(); err != nil {
		return chunkSyncStats{}, ChunkChangeSet{}, fmt.Errorf("commit transaction: %w", err)
	}
	changes.Merge(delta)
	return stats, changes, nil
}

func (f *Folio) syncPathTx(ctx context.Context, tx *sql.Tx, relPath string) (chunkSyncStats, ChunkChangeSet, error) {
	logger := f.loggerOrDefault()
	var changes ChunkChangeSet

	absPath := f.absPath(relPath)
	info, statErr := f.fs.Stat(absPath)
	if statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			deleted, ids, delErr := f.deleteFileRecords(ctx, tx, relPath)
			if delErr != nil {
				return chunkSyncStats{}, ChunkChangeSet{}, delErr
			}
			logger.Info("Removed file from index", "file_path", relPath, "deleted", deleted)
			changes.Deletions = append(changes.Deletions, ids...)
			return chunkSyncStats{deleted: deleted}, changes, nil
		}
		return chunkSyncStats{}, ChunkChangeSet{}, fmt.Errorf("stat file %s: %w", relPath, statErr)
	}

	currentMeta := fileMetadata{
		size:         info.Size(),
		mtimeNS:      info.ModTime().UnixNano(),
		chunkSize:    f.opts.ChunkSize,
		chunkOverlap: f.opts.ChunkOverlap,
	}
	if !f.force {
		storedMeta, ok, err := f.loadFileMetadata(ctx, tx, relPath)
		if err != nil {
			return chunkSyncStats{}, ChunkChangeSet{}, err
		}
		if ok && storedMeta.size == currentMeta.size && storedMeta.mtimeNS == currentMeta.mtimeNS &&
			storedMeta.chunkSize == currentMeta.chunkSize && storedMeta.chunkOverlap == currentMeta.chunkOverlap {
			return chunkSyncStats{}, ChunkChangeSet{}, nil
		}
	}

	chunks, err := f.chunkFile(relPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			deleted, ids, delErr := f.deleteFileRecords(ctx, tx, relPath)
			if delErr != nil {
				return chunkSyncStats{}, ChunkChangeSet{}, delErr
			}
			logger.Info("Removed file from index", "file_path", relPath, "deleted", deleted)
			changes.Deletions = append(changes.Deletions, ids...)
			return chunkSyncStats{deleted: deleted}, changes, nil
		}
		return chunkSyncStats{}, ChunkChangeSet{}, err
	}
	stats, err := f.syncFileChunks(ctx, tx, relPath, chunks, &changes)
	if err != nil {
		return chunkSyncStats{}, ChunkChangeSet{}, err
	}
	if err := f.upsertFileMetadata(ctx, tx, relPath, currentMeta); err != nil {
		return chunkSyncStats{}, ChunkChangeSet{}, err
	}
	logger.Info("Synced file", "file_path", relPath, "chunks", len(chunks), "inserted", stats.inserted, "updated", stats.updated, "deleted", stats.deleted)
	return stats, changes, nil
}

func (f *Folio) loggerOrDefault() *slog.Logger {
	if f.logger != nil {
		return f.logger
	}
	return slog.Default()
}

func pathMatchesIgnore(relPath string, ignore []string) bool {
	if len(ignore) == 0 {
		return false
	}
	rel := strings.TrimLeft(filepath.ToSlash(relPath), "/")
	if rel == "." {
		rel = ""
	}
	var relWithSlash string
	if rel == "" {
		relWithSlash = "/"
	} else {
		relWithSlash = "/" + rel + "/"
	}
	for _, raw := range ignore {
		entry := strings.Trim(strings.TrimSpace(raw), "/")
		if entry == "" {
			continue
		}
		entry = filepath.ToSlash(entry)
		entrySlash := "/" + entry + "/"
		if rel == entry || strings.HasPrefix(rel, entry+"/") {
			return true
		}
		if strings.Contains(relWithSlash, entrySlash) {
			return true
		}
	}
	return false
}

func (f *Folio) dispatchChunkChanges(ctx context.Context, changes ChunkChangeSet) error {
	if changes.IsEmpty() || len(f.targets) == 0 {
		return nil
	}
	for _, target := range f.targets {
		if err := target.ApplyChunkChanges(ctx, changes); err != nil {
			return err
		}
	}
	return nil
}
