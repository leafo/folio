package folio

import (
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// CollectFiles walks the root directory and returns the relative paths of files
// matching the provided set of extensions while skipping ignored directories.
func CollectFiles(filesystem FileSystem, root string, extensions, ignoreDirs []string) ([]string, error) {
	if len(extensions) == 0 {
		return nil, nil
	}

	extSet := make(map[string]struct{}, len(extensions))
	for _, ext := range extensions {
		ext = strings.ToLower(strings.TrimSpace(ext))
		if ext == "" {
			continue
		}
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		extSet[ext] = struct{}{}
	}

	if len(extSet) == 0 {
		return nil, nil
	}

	ignoreSet := make(map[string]struct{}, len(ignoreDirs))
	for _, dir := range ignoreDirs {
		clean := strings.Trim(strings.TrimSpace(dir), "/")
		if clean == "" {
			continue
		}
		ignoreSet[filepath.ToSlash(clean)] = struct{}{}
	}

	var files []string
	err := filesystem.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			relDir, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			relSlash := filepath.ToSlash(strings.Trim(relDir, "/"))
			if relSlash != "" {
				if _, ok := ignoreSet[relSlash]; ok {
					return fs.SkipDir
				}
			}
			return nil
		}
		ext := strings.ToLower(filepath.Ext(d.Name()))
		if _, ok := extSet[ext]; !ok {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(files)
	return files, nil
}
