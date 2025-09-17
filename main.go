package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

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
		showFile     string
		listFiles    bool
	)

	flag.StringVar(&root, "root", ".", "root directory to scan")
	flag.StringVar(&dbPath, "db", "folio.db", "path to the SQLite database file")
	flag.IntVar(&chunkSize, "chunk-size", 200, "number of lines per chunk")
	flag.IntVar(&chunkOverlap, "chunk-overlap", 20, "number of overlapping lines between consecutive chunks")
	flag.StringVar(&extensions, "extensions", ".txt,.md,.rst,.go,.py,.js,.ts,.tsx,.json,.yaml,.yml,.toml", "comma separated list of file extensions to include")
	flag.BoolVar(&watchMode, "watch", false, "enable watch mode to process changes continuously")
	flag.StringVar(&showFile, "show-file", "", "show stored chunks for the given file and exit")
	flag.BoolVar(&listFiles, "list", false, "list tracked files and exit")
	flag.Parse()

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)

	if showFile != "" && listFiles {
		logger.Error("Cannot use --show-file and --list at the same time")
		os.Exit(1)
	}

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

	if showFile != "" {
		absShow := showFile
		if !filepath.IsAbs(absShow) {
			absShow = filepath.Join(root, showFile)
		}
		absShow, err = filepath.Abs(absShow)
		if err != nil {
			logger.Error("Failed to resolve show file path", "file", showFile, "error", err)
			os.Exit(1)
		}
		rel, relErr := filepath.Rel(root, absShow)
		if relErr != nil {
			logger.Error("Failed to compute relative path for show file", "file", showFile, "error", relErr)
			os.Exit(1)
		}
		relSlash := filepath.ToSlash(rel)
		if relSlash == ".." || strings.HasPrefix(relSlash, "../") {
			logger.Error("Show file must be within the root directory", "file", showFile, "root", root)
			os.Exit(1)
		}
		showFile = relSlash
	}

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

	if listFiles {
		if err := renderStoredFiles(ctx, manager); err != nil {
			logger.Error("Failed to list stored files", "error", err)
			os.Exit(1)
		}
		return
	}

	if showFile != "" {
		if err := renderStoredChunks(ctx, manager, showFile); err != nil {
			logger.Error("Failed to display stored chunks", "file", showFile, "error", err)
			os.Exit(1)
		}
		return
	}

	logger.Info("Launching synchronization", "root", root)
	summary, err := manager.Synchronize(ctx)
	if err != nil {
		logger.Error("Synchronization failed", "error", err)
		os.Exit(1)
	}

	printSummary(summary)

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

func renderStoredFiles(ctx context.Context, manager *folio.Folio) error {
	files, err := manager.StoredFiles(ctx)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fmt.Fprintln(os.Stdout, "No files stored in the database")
		return nil
	}

	fmt.Fprintf(os.Stdout, "Stored files (%d)\n", len(files))
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "UPDATED AT\tCHUNKS\tFILE")
	for _, file := range files {
		fmt.Fprintf(tw, "%s\t%d\t%s\n", file.UpdatedAt, file.Chunks, file.FilePath)
	}

	return nil
}

func renderStoredChunks(ctx context.Context, manager *folio.Folio, filePath string) error {
	chunks, err := manager.StoredChunks(ctx, filePath)
	if err != nil {
		return err
	}

	if len(chunks) == 0 {
		fmt.Fprintf(os.Stdout, "No chunks stored for %s\n", filePath)
		return nil
	}

	divider := strings.Repeat("-", 80)
	fmt.Fprintf(os.Stdout, "Stored chunks for %s (%d chunks)\n", filePath, len(chunks))
	for idx, chunk := range chunks {
		fmt.Fprintf(os.Stdout, "\n[%d] lines %d-%d\n", idx+1, chunk.StartLine, chunk.EndLine)
		fmt.Fprintf(os.Stdout, "hash: %s | updated: %s\n", chunk.ContentHash, chunk.UpdatedAt)
		fmt.Fprintln(os.Stdout, divider)
		if chunk.Content != "" {
			fmt.Fprintln(os.Stdout, chunk.Content)
		}
		fmt.Fprintln(os.Stdout, divider)
	}

	return nil
}

func printSummary(summary folio.SyncSummary) {
	fmt.Fprintln(os.Stdout, "synchronization complete")
	fmt.Fprintf(os.Stdout, "total files: %d\n", summary.TotalFiles)
	fmt.Fprintf(os.Stdout, "total chunks: %d\n", summary.TotalChunks)
	fmt.Fprintf(os.Stdout, "chunks inserted: %d\n", summary.ChunksInserted)
	fmt.Fprintf(os.Stdout, "chunks updated: %d\n", summary.ChunksUpdated)
	fmt.Fprintf(os.Stdout, "chunks deleted: %d\n", summary.ChunksDeleted)
}
