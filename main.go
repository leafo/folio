package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/leafo/folio/internal/folio"
)

const configFileName = "folio.json"

func main() {
	cfg := defaultConfig()
	configLoaded, err := loadConfigFile(configFileName, &cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load %s: %v\n", configFileName, err)
		os.Exit(1)
	}
	cfg.IgnoreDirs = normalizeIgnoreDirs(cfg.IgnoreDirs)

	var (
		root              string
		dbPath            string
		chunkSize         int
		chunkOverlap      int
		extensions        string
		watchMode         bool
		showFile          string
		listFiles         bool
		writeConfig       bool
		ignoreDirectories string
	)

	defaultExtensions := strings.Join(cfg.Extensions, ",")
	defaultIgnore := strings.Join(cfg.IgnoreDirs, ",")

	flag.StringVar(&root, "root", cfg.Root, "root directory to scan")
	flag.StringVar(&dbPath, "db", cfg.DBPath, "path to the SQLite database file")
	flag.IntVar(&chunkSize, "chunk-size", cfg.ChunkSize, "number of lines per chunk")
	flag.IntVar(&chunkOverlap, "chunk-overlap", cfg.ChunkOverlap, "number of overlapping lines between consecutive chunks")
	flag.StringVar(&extensions, "extensions", defaultExtensions, "comma separated list of file extensions to include")
	flag.StringVar(&ignoreDirectories, "ignore-directories", defaultIgnore, "comma separated list of directories to ignore")
	flag.BoolVar(&watchMode, "watch", false, "enable watch mode to process changes continuously")
	flag.StringVar(&showFile, "show-file", "", "show stored chunks for the given file and exit")
	flag.BoolVar(&listFiles, "list", false, "list tracked files and exit")
	flag.BoolVar(&writeConfig, "write-config", false, "write configuration to folio.json and exit")
	flag.Parse()

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)
	if configLoaded {
		logger.Info("Loaded configuration file", "path", configFileName)
	}

	if showFile != "" && listFiles {
		logger.Error("Cannot use --show-file and --list at the same time")
		os.Exit(1)
	}
	if writeConfig && (showFile != "" || listFiles) {
		logger.Error("Cannot combine --write-config with --show-file or --list")
		os.Exit(1)
	}

	extList := parseExtensions(extensions)
	if len(extList) == 0 {
		logger.Error("No extensions provided")
		os.Exit(1)
	}

	ignoreList := normalizeIgnoreDirs(parseList(ignoreDirectories))
	cfg.IgnoreDirs = ignoreList

	cfg.Root = root
	cfg.DBPath = dbPath
	cfg.ChunkSize = chunkSize
	cfg.ChunkOverlap = chunkOverlap
	cfg.Extensions = extList

	if cfg.ChunkSize <= 0 {
		logger.Error("Chunk size must be positive", "chunk_size", cfg.ChunkSize)
		os.Exit(1)
	}
	if cfg.ChunkOverlap < 0 {
		logger.Error("Chunk overlap cannot be negative", "chunk_overlap", cfg.ChunkOverlap)
		os.Exit(1)
	}
	if cfg.ChunkOverlap >= cfg.ChunkSize {
		logger.Warn("Chunk overlap exceeds chunk size; adjusting", "requested_overlap", cfg.ChunkOverlap, "chunk_size", cfg.ChunkSize, "adjusted_overlap", cfg.ChunkSize-1)
		cfg.ChunkOverlap = cfg.ChunkSize - 1
	}

	absRoot, err := filepath.Abs(cfg.Root)
	if err != nil {
		logger.Error("Failed to resolve root", "root", cfg.Root, "error", err)
		os.Exit(1)
	}
	rootAbs := absRoot

	if showFile != "" {
		absShow := showFile
		if !filepath.IsAbs(absShow) {
			absShow = filepath.Join(rootAbs, showFile)
		}
		absShow, err = filepath.Abs(absShow)
		if err != nil {
			logger.Error("Failed to resolve show file path", "file", showFile, "error", err)
			os.Exit(1)
		}
		rel, relErr := filepath.Rel(rootAbs, absShow)
		if relErr != nil {
			logger.Error("Failed to compute relative path for show file", "file", showFile, "error", relErr)
			os.Exit(1)
		}
		relSlash := filepath.ToSlash(rel)
		if relSlash == ".." || strings.HasPrefix(relSlash, "../") {
			logger.Error("Show file must be within the root directory", "file", showFile, "root", rootAbs)
			os.Exit(1)
		}
		showFile = relSlash
	}

	ctx := context.Background()

	if writeConfig {
		if err := writeConfigFile(configFileName, cfg); err != nil {
			logger.Error("Failed to write configuration", "error", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "wrote configuration to %s\n", configFileName)
		return
	}

	db, err := folio.OpenDatabase(ctx, cfg.DBPath)
	if err != nil {
		logger.Error("Failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	logger.Info("Opened database", "path", cfg.DBPath)

	manager := folio.NewFolio(db, rootAbs, cfg.Options(), logger)

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

	logger.Info("Launching synchronization", "root", rootAbs)
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
	}
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

func defaultConfig() folio.Config {
	return folio.Config{
		Root:         ".",
		DBPath:       "folio.db",
		Extensions:   []string{".txt", ".md", ".rst", ".go", ".py", ".js", ".ts", ".tsx", ".json", ".yaml", ".yml", ".toml"},
		ChunkSize:    200,
		ChunkOverlap: 20,
		IgnoreDirs:   []string{".git", "node_modules"},
	}
}

func loadConfigFile(path string, cfg *folio.Config) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return false, fmt.Errorf("parse config: %w", err)
	}
	return true, nil
}

func writeConfigFile(path string, cfg folio.Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func parseList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	var result []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}
	return result
}

func normalizeIgnoreDirs(dirs []string) []string {
	if len(dirs) == 0 {
		return nil
	}
	norm := make([]string, 0, len(dirs))
	seen := make(map[string]struct{})
	for _, d := range dirs {
		clean := strings.Trim(strings.TrimSpace(d), "/")
		clean = filepath.ToSlash(clean)
		if clean == "" {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		norm = append(norm, clean)
	}
	return norm
}
