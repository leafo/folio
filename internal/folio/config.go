package folio

import (
	"os"
	"strings"
)

// Config captures Folio settings that control database construction and chunking.
type Config struct {
	Root         string             `json:"root"`
	DBPath       string             `json:"db"`
	Extensions   []string           `json:"extensions"`
	ChunkSize    int                `json:"chunk_size"`
	ChunkOverlap int                `json:"chunk_overlap"`
	IgnoreDirs   []string           `json:"ignore_directories"`
	Meilisearch  *MeilisearchConfig `json:"meilisearch,omitempty"`
	ShellTarget  *ShellTargetConfig `json:"shell_target,omitempty"`
}

// MeilisearchConfig captures connection settings for optional search synchronization.
type MeilisearchConfig struct {
	Host   string `json:"host,omitempty"`
	APIKey string `json:"api_key,omitempty"`
	Index  string `json:"index,omitempty"`
}

func (c *MeilisearchConfig) applyDefaults() {
	if c.Host == "" {
		c.Host = "http://localhost:7700"
	}
	if env := os.Getenv("MEILISEARCH_API_KEY"); env != "" && c.APIKey == "" {
		c.APIKey = env
	}
}

func (c *MeilisearchConfig) IsEmpty() bool {
	if c == nil {
		return true
	}
	return strings.TrimSpace(c.Host) == "" &&
		strings.TrimSpace(c.APIKey) == "" &&
		strings.TrimSpace(c.Index) == ""
}

// Options converts the configuration into the Folio options used by the manager.
func (c Config) Options() Options {
	var meiliConfig MeilisearchConfig
	if c.Meilisearch != nil {
		meiliConfig = *c.Meilisearch
	}
	(&meiliConfig).applyDefaults()

	var shellCfg *ShellTargetConfig
	if c.ShellTarget != nil && !c.ShellTarget.IsEmpty() {
		copyCfg := *c.ShellTarget
		shellCfg = &copyCfg
	}

	return Options{
		Extensions:   append([]string(nil), c.Extensions...),
		ChunkSize:    c.ChunkSize,
		ChunkOverlap: c.ChunkOverlap,
		IgnoreDirs:   append([]string(nil), c.IgnoreDirs...),
		Meilisearch:  meiliConfig,
		ShellTarget:  shellCfg,
	}
}

// ShellTargetConfig configures the shell command sync target.
type ShellTargetConfig struct {
	Command string `json:"command,omitempty"`
}

func (c *ShellTargetConfig) IsEmpty() bool {
	if c == nil {
		return true
	}
	return strings.TrimSpace(c.Command) == ""
}
