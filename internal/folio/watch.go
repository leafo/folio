package folio

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

const watchDebounceInterval = 150 * time.Millisecond

// WatchAndSync monitors the root directory for changes and incrementally
// updates the SQLite database when filesystem events settle.
func (f *Folio) WatchAndSync(ctx context.Context) error {
	logger := f.loggerOrDefault()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer watcher.Close()

	watched := make(map[string]struct{})
	if err := f.addRecursiveWatch(watcher, f.root, watched); err != nil {
		return err
	}

	logger.Info("Watch mode active", "root", f.root, "debounce", watchDebounceInterval.String())

	extSet := makeExtensionSet(f.opts.Extensions)

	var debounceTimer *time.Timer
	changedFiles := make(map[string]struct{})
	removedDirs := make(map[string]struct{})

	for {
		var debounceC <-chan time.Time
		if debounceTimer != nil {
			debounceC = debounceTimer.C
		}

		select {
		case <-ctx.Done():
			logger.Info("Stopping watch mode", "reason", ctx.Err())
			return ctx.Err()
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			f.handleWatcherEvent(event, watcher, watched, extSet, &debounceTimer, changedFiles, removedDirs)
		case err, ok := <-watcher.Errors:
			if !ok || err == nil {
				continue
			}
			logger.Error("Watcher error", "error", err)
		case <-debounceC:
			stopTimer(&debounceTimer)
			if len(changedFiles) == 0 && len(removedDirs) == 0 {
				continue
			}
			if syncErr := f.applyIncrementalChanges(ctx, changedFiles, removedDirs); syncErr != nil {
				logger.Error("Incremental synchronization failed", "error", syncErr)
				if errors.Is(syncErr, context.Canceled) {
					return syncErr
				}
				continue
			}
			changedFiles = make(map[string]struct{})
			removedDirs = make(map[string]struct{})
		}
	}
}

func (f *Folio) handleWatcherEvent(event fsnotify.Event, watcher *fsnotify.Watcher, watched map[string]struct{}, extSet map[string]struct{}, debounceTimer **time.Timer, changed map[string]struct{}, removedDirs map[string]struct{}) {
	logger := f.loggerOrDefault()

	path := filepath.Clean(event.Name)
	rel := f.relativePath(path)
	// logger.Info("Filesystem event", "op", event.Op.String(), "path", rel)

	dirChanged := false

	if event.Op&fsnotify.Create != 0 {
		info, err := f.fs.Stat(path)
		if err == nil && info.IsDir() {
			if f.isIgnored(rel) {
				return
			}
			if err := f.addRecursiveWatch(watcher, path, watched); err != nil {
				logger.Error("Failed to watch new directory", "path", rel, "error", err)
			}
			delete(removedDirs, rel)
			dirChanged = true
		}
	}

	if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		if _, ok := watched[path]; ok {
			if err := watcher.Remove(path); err != nil {
				logger.Error("Failed to stop watching directory", "path", rel, "error", err)
			}
			delete(watched, path)
			logger.Info("Stopped watching directory", "path", rel)
			if rel != "." {
				removedDirs[rel] = struct{}{}
			}
			dirChanged = true
		}
	}

	if dirChanged {
		scheduleSync(debounceTimer)
		return
	}

	if !shouldTriggerSync(event.Op) {
		return
	}

	if f.isIgnored(rel) {
		return
	}

	if !isRelevantFile(path, extSet) {
		return
	}

	changed[rel] = struct{}{}
	// logger.Info("Queueing synchronization for file", "path", rel, "op", event.Op.String())

	scheduleSync(debounceTimer)
}

func (f *Folio) addRecursiveWatch(watcher *fsnotify.Watcher, start string, watched map[string]struct{}) error {
	logger := f.loggerOrDefault()

	return f.fs.WalkDir(start, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		clean := filepath.Clean(path)
		rel := f.relativePath(clean)
		if f.isIgnored(rel) {
			return fs.SkipDir
		}
		if _, ok := watched[clean]; ok {
			return nil
		}
		if err := watcher.Add(clean); err != nil {
			return fmt.Errorf("watch directory %s: %w", clean, err)
		}
		watched[clean] = struct{}{}
		logger.Info("Watching directory", "path", rel)
		return nil
	})
}

func (f *Folio) applyIncrementalChanges(ctx context.Context, changed, removedDirs map[string]struct{}) error {
	if len(changed) == 0 && len(removedDirs) == 0 {
		return nil
	}

	logger := f.loggerOrDefault()

	changedList := make([]string, 0, len(changed))
	for rel := range changed {
		changedList = append(changedList, rel)
	}
	sort.Strings(changedList)

	removedDirList := make([]string, 0, len(removedDirs))
	for rel := range removedDirs {
		if rel == "." || rel == "" {
			continue
		}
		removedDirList = append(removedDirList, rel)
	}
	sort.Strings(removedDirList)

	totals := chunkSyncStats{}
	var changes ChunkChangeSet

	for _, rel := range changedList {
		stats, delta, syncErr := f.SyncPath(ctx, rel)
		if syncErr != nil {
			return syncErr
		}
		totals.inserted += stats.inserted
		totals.updated += stats.updated
		totals.deleted += stats.deleted
		changes.Merge(delta)
	}

	for _, rel := range removedDirList {
		files, listErr := f.listFilesInDirectory(ctx, rel)
		if listErr != nil {
			return listErr
		}
		dirDeleted := 0
		for _, file := range files {
			stats, delta, syncErr := f.SyncPath(ctx, file)
			if syncErr != nil {
				return syncErr
			}
			dirDeleted += stats.deleted
			totals.deleted += stats.deleted
			changes.Merge(delta)
		}
		logger.Debug("Removed directory from index", "directory", rel, "files", len(files), "chunks", dirDeleted)
	}

	logger.Debug("Incremental synchronization summary", "updated_files", len(changedList), "removed_dirs", len(removedDirList), "chunks_inserted", totals.inserted, "chunks_updated", totals.updated, "chunks_deleted", totals.deleted)

	return f.dispatchChunkChanges(ctx, changes)
}

func makeExtensionSet(exts []string) map[string]struct{} {
	set := make(map[string]struct{}, len(exts))
	for _, ext := range exts {
		set[strings.ToLower(ext)] = struct{}{}
	}
	return set
}

func isRelevantFile(path string, extSet map[string]struct{}) bool {
	ext := strings.ToLower(filepath.Ext(path))
	if ext == "" {
		return false
	}
	_, ok := extSet[ext]
	return ok
}

func shouldTriggerSync(op fsnotify.Op) bool {
	return op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) != 0
}

func scheduleSync(timer **time.Timer) {
	if *timer == nil {
		*timer = time.NewTimer(watchDebounceInterval)
		return
	}
	if !(*timer).Stop() {
		select {
		case <-(*timer).C:
		default:
		}
	}
	(*timer).Reset(watchDebounceInterval)
}

func stopTimer(timer **time.Timer) {
	if *timer == nil {
		return
	}
	if !(*timer).Stop() {
		select {
		case <-(*timer).C:
		default:
		}
	}
	*timer = nil
}
