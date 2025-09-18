package folio

// Config captures Folio settings that control database construction and chunking.
type Config struct {
	Root         string   `json:"root"`
	DBPath       string   `json:"db"`
	Extensions   []string `json:"extensions"`
	ChunkSize    int      `json:"chunk_size"`
	ChunkOverlap int      `json:"chunk_overlap"`
	IgnoreDirs   []string `json:"ignore_directories"`
	Meilisearch  MeilisearchConfig `json:"meilisearch"`
}

// MeilisearchConfig captures connection settings for optional search synchronization.
type MeilisearchConfig struct {
	Host    string `json:"host"`
	APIKey  string `json:"api_key"`
	Index   string `json:"index"`
}

// Options converts the configuration into the Folio options used by the manager.
func (c Config) Options() Options {
	return Options{
		Extensions:   append([]string(nil), c.Extensions...),
		ChunkSize:    c.ChunkSize,
		ChunkOverlap: c.ChunkOverlap,
		IgnoreDirs:   append([]string(nil), c.IgnoreDirs...),
		Meilisearch:  c.Meilisearch,
	}
}
