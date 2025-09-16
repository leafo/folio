package folio

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
)

// Chunk represents a slice of a source file that will be embedded.
type Chunk struct {
	FilePath    string
	StartLine   int
	EndLine     int
	Content     string
	ContentHash string
}

// ChunkOptions controls how files are chunked into smaller sections.
type ChunkOptions struct {
	ChunkSize    int
	ChunkOverlap int
}

func (f *Folio) chunkFile(relPath string) ([]Chunk, error) {
	opts := f.chunkOptions()
	if opts.ChunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be positive")
	}
	if opts.ChunkOverlap < 0 {
		return nil, fmt.Errorf("chunk overlap cannot be negative")
	}
	if opts.ChunkOverlap >= opts.ChunkSize {
		opts.ChunkOverlap = opts.ChunkSize - 1
	}

	absPath := f.absPath(relPath)
	file, err := f.fs.Open(absPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	defer file.Close()

	lines, err := readLines(file)
	if err != nil {
		return nil, err
	}

	if len(lines) == 0 {
		return nil, nil
	}

	step := opts.ChunkSize - opts.ChunkOverlap
	if step <= 0 {
		step = 1
	}

	var chunks []Chunk
	for start := 0; start < len(lines); start += step {
		end := start + opts.ChunkSize
		if end > len(lines) {
			end = len(lines)
		}

		content := strings.Join(lines[start:end], "\n")
		sum := md5.Sum([]byte(content))

		chunk := Chunk{
			FilePath:    filepath.ToSlash(relPath),
			StartLine:   start + 1,
			EndLine:     end,
			Content:     content,
			ContentHash: hex.EncodeToString(sum[:]),
		}
		chunks = append(chunks, chunk)

		if end == len(lines) {
			break
		}
	}

	return chunks, nil
}

func readLines(r io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}
