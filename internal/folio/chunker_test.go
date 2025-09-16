package folio

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

type mapFileSystem struct {
	fs fstest.MapFS
}

func newMapFileSystem(files map[string]string) mapFileSystem {
	m := make(fstest.MapFS, len(files))
	for name, data := range files {
		m[normalizePath(name)] = &fstest.MapFile{Mode: 0o644, Data: []byte(data)}
	}
	if len(m) == 0 {
		m["."] = &fstest.MapFile{Mode: fs.ModeDir}
	}
	return mapFileSystem{fs: m}
}

func (m mapFileSystem) Open(name string) (fs.File, error) {
	return m.fs.Open(normalizePath(name))
}

func (m mapFileSystem) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(m.fs, normalizePath(name))
}

func (m mapFileSystem) WalkDir(root string, fn fs.WalkDirFunc) error {
	path := normalizePath(root)
	if path == "" {
		path = "."
	}
	return fs.WalkDir(m.fs, path, fn)
}

func normalizePath(p string) string {
	if p == "" {
		return ""
	}
	cleaned := filepath.ToSlash(p)
	cleaned = strings.TrimPrefix(cleaned, "./")
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" {
		return "."
	}
	return cleaned
}

func TestChunkFileProducesExpectedChunks(t *testing.T) {
	fsMap := newMapFileSystem(map[string]string{
		"sample.txt": strings.Join([]string{
			"line1",
			"line2",
			"line3",
			"line4",
			"line5",
			"line6",
			"line7",
		}, "\n"),
	})

	folio := &Folio{
		root: "",
		opts: Options{ChunkSize: 3, ChunkOverlap: 1},
		fs:   fsMap,
	}

	chunks, err := folio.chunkFile("sample.txt")
	if err != nil {
		t.Fatalf("chunkFile returned error: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	expectedRanges := [][2]int{{1, 3}, {3, 5}, {5, 7}}
	expectedContents := []string{
		"line1\nline2\nline3",
		"line3\nline4\nline5",
		"line5\nline6\nline7",
	}

	for i, chunk := range chunks {
		if chunk.StartLine != expectedRanges[i][0] || chunk.EndLine != expectedRanges[i][1] {
			t.Fatalf("chunk %d range mismatch: expected %v got (%d,%d)", i, expectedRanges[i], chunk.StartLine, chunk.EndLine)
		}
		if chunk.Content != expectedContents[i] {
			t.Fatalf("chunk %d content mismatch: expected %q got %q", i, expectedContents[i], chunk.Content)
		}
		if chunk.ContentHash == "" {
			t.Fatalf("chunk %d expected non-empty hash", i)
		}
	}
}

func TestChunkFileOverlapAdjusted(t *testing.T) {
	fsMap := newMapFileSystem(map[string]string{
		"overlap.txt": strings.Join([]string{
			"A",
			"B",
			"C",
			"D",
		}, "\n"),
	})

	folio := &Folio{
		root: "",
		opts: Options{ChunkSize: 3, ChunkOverlap: 5},
		fs:   fsMap,
	}

	chunks, err := folio.chunkFile("overlap.txt")
	if err != nil {
		t.Fatalf("chunkFile returned error: %v", err)
	}

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks due to adjusted overlap, got %d", len(chunks))
	}

	if chunks[1].StartLine != 2 {
		t.Fatalf("expected second chunk to start at line 2 due to overlap adjustment, got %d", chunks[1].StartLine)
	}
}

func TestChunkFileValidatesParameters(t *testing.T) {
	folio := &Folio{
		root: "",
		opts: Options{ChunkSize: 0, ChunkOverlap: 0},
		fs:   newMapFileSystem(nil),
	}

	if _, err := folio.chunkFile("missing.txt"); err == nil {
		t.Fatalf("expected error when chunk size is zero")
	}
}

func TestChunkFileEmptyContent(t *testing.T) {
	fsMap := newMapFileSystem(map[string]string{
		"empty.txt": "",
	})

	folio := &Folio{
		root: "",
		opts: Options{ChunkSize: 4, ChunkOverlap: 1},
		fs:   fsMap,
	}

	chunks, err := folio.chunkFile("empty.txt")
	if err != nil {
		t.Fatalf("chunkFile returned error: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("expected no chunks for empty file, got %d", len(chunks))
	}
}
