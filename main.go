package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/leafo/folio/internal/folio"
)

func main() {
	var (
		root         string
		dbPath       string
		chunkSize    int
		chunkOverlap int
		extensions   string
		watchMode    bool
	)

	flag.StringVar(&root, "root", ".", "root directory to scan")
	flag.StringVar(&dbPath, "db", "folio.db", "path to the SQLite database file")
	flag.IntVar(&chunkSize, "chunk-size", 200, "number of lines per chunk")
	flag.IntVar(&chunkOverlap, "chunk-overlap", 20, "number of overlapping lines between consecutive chunks")
	flag.StringVar(&extensions, "extensions", ".txt,.md,.rst,.go,.py,.js,.ts,.tsx,.json,.yaml,.yml,.toml", "comma separated list of file extensions to include")
	flag.BoolVar(&watchMode, "watch", false, "enable watch mode to process changes continuously")
	flag.Parse()

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)

	extList := parseExtensions(extensions)
	if len(extList) == 0 {
		logger.Error("No extensions provided")
		os.Exit(1)
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		logger.Error("Failed to resolve root", "root", root, "error", err)
		os.Exit(1)
	}
	root = absRoot

	if chunkSize <= 0 {
		logger.Error("Chunk size must be positive", "chunk_size", chunkSize)
		os.Exit(1)
	}
	if chunkOverlap < 0 {
		logger.Error("Chunk overlap cannot be negative", "chunk_overlap", chunkOverlap)
		os.Exit(1)
	}
	if chunkOverlap >= chunkSize {
		logger.Warn("Chunk overlap exceeds chunk size; adjusting", "requested_overlap", chunkOverlap, "chunk_size", chunkSize, "adjusted_overlap", chunkSize-1)
		chunkOverlap = chunkSize - 1
	}

	ctx := context.Background()

	db, err := folio.OpenDatabase(ctx, dbPath)
	if err != nil {
		logger.Error("Failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	logger.Info("Opened database", "path", dbPath)

	opts := folio.Options{
		Extensions:   extList,
		ChunkSize:    chunkSize,
		ChunkOverlap: chunkOverlap,
	}

	manager := folio.NewFolio(db, root, opts, logger)

	logger.Info("Launching synchronization", "root", root)
	if err := manager.Synchronize(ctx); err != nil {
		logger.Error("Synchronization failed", "error", err)
		os.Exit(1)
	}

	if watchMode {
		logger.Info("Entering watch mode")
		if err := manager.WatchAndSync(ctx); err != nil {
			logger.Error("Watch mode terminated", "error", err)
			os.Exit(1)
		}
		return
	}

	fmt.Fprintln(os.Stdout, "synchronization complete")
}

func parseExtensions(raw string) []string {
	parts := strings.Split(raw, ",")
	var result []string
	for _, part := range parts {
		ext := strings.TrimSpace(part)
		if ext == "" {
			continue
		}
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		result = append(result, ext)
	}
	return result
}
