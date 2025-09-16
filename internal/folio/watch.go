package folio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"log/slog"
)

const watchDebounceInterval = 750 * time.Millisecond

// WatchAndSync monitors the root directory for changes and periodically runs
// Synchronize after filesystem events settle.
func WatchAndSync(ctx context.Context, db *sql.DB, root string, opts Options) error {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer watcher.Close()

	watched := make(map[string]struct{})
	if err := addRecursiveWatch(watcher, root, root, watched, logger); err != nil {
		return err
	}

	logger.Info("Watch mode active", "root", root, "debounce", watchDebounceInterval.String())

	extSet := makeExtensionSet(opts.Extensions)

	var debounceTimer *time.Timer

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
			handleWatcherEvent(event, watcher, root, watched, logger, extSet, &debounceTimer)
		case err, ok := <-watcher.Errors:
			if !ok {
				continue
			}
			if err == nil {
				continue
			}
			logger.Error("Watcher error", "error", err)
		case <-debounceC:
			stopTimer(&debounceTimer)
			logger.Info("Filesystem settled; running synchronization")
			if syncErr := Synchronize(ctx, db, root, opts); syncErr != nil {
				logger.Error("Synchronization failed during watch", "error", syncErr)
				if !errors.Is(syncErr, context.Canceled) {
					continue
				}
				return syncErr
			}
			logger.Info("Synchronization completed for filesystem changes")
		}
	}
}

func handleWatcherEvent(event fsnotify.Event, watcher *fsnotify.Watcher, root string, watched map[string]struct{}, logger *slog.Logger, extSet map[string]struct{}, debounceTimer **time.Timer) {
	path := filepath.Clean(event.Name)
	rel := relativePath(root, path)
	logger.Info("Filesystem event", "op", event.Op.String(), "path", rel)

	dirChanged := false

	if event.Op&fsnotify.Create != 0 {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			if err := addRecursiveWatch(watcher, root, path, watched, logger); err != nil {
				logger.Error("Failed to watch new directory", "path", rel, "error", err)
			}
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

	if !isRelevantFile(path, extSet) {
		return
	}

	logger.Info("Queueing synchronization for changed file", "path", rel)
	scheduleSync(debounceTimer)
}

func addRecursiveWatch(watcher *fsnotify.Watcher, root, start string, watched map[string]struct{}, logger *slog.Logger) error {
	return filepath.WalkDir(start, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		clean := filepath.Clean(path)
		if _, ok := watched[clean]; ok {
			return nil
		}
		if err := watcher.Add(clean); err != nil {
			return fmt.Errorf("watch directory %s: %w", clean, err)
		}
		watched[clean] = struct{}{}
		logger.Info("Watching directory", "path", relativePath(root, clean))
		return nil
	})
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

func relativePath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}
