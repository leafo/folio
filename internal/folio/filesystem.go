package folio

import (
	"io/fs"
	"os"
	"path/filepath"
)

// FileSystem abstracts filesystem interactions so tests can provide in-memory implementations.
type FileSystem interface {
	Open(name string) (fs.File, error)
	Stat(name string) (fs.FileInfo, error)
	WalkDir(root string, fn fs.WalkDirFunc) error
}

// OSFileSystem implements FileSystem using the local OS filesystem.
type OSFileSystem struct{}

func (OSFileSystem) Open(name string) (fs.File, error) {
	return os.Open(name)
}

func (OSFileSystem) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(name)
}

func (OSFileSystem) WalkDir(root string, fn fs.WalkDirFunc) error {
	return filepath.WalkDir(root, fn)
}
