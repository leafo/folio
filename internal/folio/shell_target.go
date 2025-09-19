package folio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

type shellTarget struct {
	command string
}

func newShellTarget(cfg *ShellTargetConfig) ChunkSyncTarget {
	if cfg == nil || cfg.IsEmpty() {
		return nil
	}
	cmd := strings.TrimSpace(cfg.Command)
	if cmd == "" {
		return nil
	}
	return &shellTarget{command: cmd}
}

func (s *shellTarget) ApplyChunkChanges(ctx context.Context, changes ChunkChangeSet) error {
	if changes.IsEmpty() {
		return nil
	}

	docs := struct {
		Upserts   []Chunk           `json:"upserts"`
		Deletions []ChunkIdentifier `json:"deletions"`
	}{
		Upserts:   changes.Upserts,
		Deletions: changes.Deletions,
	}

	payload, err := json.Marshal(docs)
	if err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", s.command)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	if _, err := io.Copy(stdin, bytes.NewReader(payload)); err != nil {
		stdin.Close()
		return err
	}
	if err := stdin.Close(); err != nil {
		return err
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("shell target failed: %w: %s", err, string(output))
	}

	return nil
}
