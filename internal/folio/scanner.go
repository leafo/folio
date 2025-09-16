package folio

import (
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// CollectFiles walks the root directory and returns the relative paths of files
// matching the provided set of extensions.
func CollectFiles(root string, extensions []string) ([]string, error) {
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

	var files []string
    err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
        if err != nil {
            return err
        }
        if d.IsDir() {
            if d.Name() == ".git" {
                return fs.SkipDir
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
