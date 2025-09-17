package folio

// Config captures Folio settings that control database construction and chunking.
type Config struct {
	Root         string   `json:"root"`
	DBPath       string   `json:"db"`
	Extensions   []string `json:"extensions"`
	ChunkSize    int      `json:"chunk_size"`
	ChunkOverlap int      `json:"chunk_overlap"`
}

// Options converts the configuration into the Folio options used by the manager.
func (c Config) Options() Options {
	return Options{
		Extensions:   append([]string(nil), c.Extensions...),
		ChunkSize:    c.ChunkSize,
		ChunkOverlap: c.ChunkOverlap,
	}
}
