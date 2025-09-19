package folio

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/meilisearch/meilisearch-go"
	"log/slog"
)

type meilisearchTarget struct {
	client *meilisearch.Client
	index  *meilisearch.Index
	logger *slog.Logger
}

func (f *Folio) initMeilisearch() {
	target, err := newMeilisearchTarget(context.Background(), f.opts.Meilisearch, f.loggerOrDefault())
	if err != nil {
		f.loggerOrDefault().Warn("Failed to initialize Meilisearch", "error", err)
		return
	}
	if target == nil {
		return
	}
	f.RegisterSyncTarget(target)
}

func newMeilisearchTarget(ctx context.Context, cfg MeilisearchConfig, logger *slog.Logger) (ChunkSyncTarget, error) {
	host := strings.TrimSpace(cfg.Host)
	indexName := strings.TrimSpace(cfg.Index)
	if indexName == "" {
		return nil, nil
	}
	if host == "" {
		host = "http://localhost:7700"
	}

	client := meilisearch.NewClient(meilisearch.ClientConfig{
		Host:   host,
		APIKey: strings.TrimSpace(cfg.APIKey),
	})
	index := client.Index(indexName)

	t := &meilisearchTarget{client: client, index: index, logger: logger}
	if err := t.ensureIndex(ctx, indexName); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *meilisearchTarget) ensureIndex(ctx context.Context, indexName string) error {
	_, err := t.client.GetIndex(indexName)
	if err != nil {
		var meiliErr *meilisearch.Error
		if errors.As(err, &meiliErr) && meiliErr.MeilisearchApiError.Code == "index_not_found" {
			task, createErr := t.client.CreateIndex(&meilisearch.IndexConfig{Uid: indexName})
			if createErr != nil {
				return createErr
			}
			if err := t.waitForTask(ctx, task); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	desiredSearchable := []string{"content", "file_path"}
	if err := t.ensureSearchableAttributes(ctx, desiredSearchable); err != nil {
		return err
	}

	desiredFilterable := []string{"file_path"}
	if err := t.ensureFilterableAttributes(ctx, desiredFilterable); err != nil {
		return err
	}

	return nil
}

func (t *meilisearchTarget) ensureSearchableAttributes(ctx context.Context, desired []string) error {
	currentPtr, err := t.index.GetSearchableAttributes()
	if err != nil {
		return err
	}
	if stringSlicesEqual(derefSlice(currentPtr), desired) {
		return nil
	}
	task, err := t.index.UpdateSearchableAttributes(&desired)
	if err != nil {
		return err
	}
	return t.waitForTask(ctx, task)
}

func (t *meilisearchTarget) ensureFilterableAttributes(ctx context.Context, desired []string) error {
	currentPtr, err := t.index.GetFilterableAttributes()
	if err != nil {
		return err
	}
	if stringSlicesEqual(derefSlice(currentPtr), desired) {
		return nil
	}
	task, err := t.index.UpdateFilterableAttributes(&desired)
	if err != nil {
		return err
	}
	return t.waitForTask(ctx, task)
}

func (t *meilisearchTarget) waitForTask(ctx context.Context, task *meilisearch.TaskInfo) error {
	if task == nil || task.TaskUID == 0 {
		return nil
	}
	_, err := t.client.WaitForTask(task.TaskUID, meilisearch.WaitParams{Context: ctx})
	return err
}

// ApplyChunkChanges satisfies the ChunkSyncTarget interface.
func (t *meilisearchTarget) ApplyChunkChanges(ctx context.Context, changes ChunkChangeSet) error {
	if changes.IsEmpty() {
		return nil
	}

	if len(changes.Upserts) > 0 {
		docs := makeMeiliDocuments(changes.Upserts)
		task, err := t.index.AddDocuments(docs)
		if err != nil {
			return err
		}
		if err := t.waitForTask(ctx, task); err != nil {
			return err
		}
	}

	if len(changes.Deletions) > 0 {
		ids := make([]string, 0, len(changes.Deletions))
		for _, id := range changes.Deletions {
			ids = append(ids, chunkDocumentID(id.FilePath, id.StartLine, id.EndLine))
		}
		task, err := t.index.DeleteDocuments(ids)
		if err != nil {
			return err
		}
		if err := t.waitForTask(ctx, task); err != nil {
			return err
		}
	}

	return nil
}

func makeMeiliDocuments(chunks []Chunk) []meiliChunkDocument {
	docs := make([]meiliChunkDocument, 0, len(chunks))
	for _, chunk := range chunks {
		docs = append(docs, meiliChunkDocument{
			ID:          chunkDocumentID(chunk.FilePath, chunk.StartLine, chunk.EndLine),
			FilePath:    chunk.FilePath,
			StartLine:   chunk.StartLine,
			EndLine:     chunk.EndLine,
			Content:     chunk.Content,
			ContentHash: chunk.ContentHash,
		})
	}
	return docs
}

func chunkDocumentID(path string, start, end int) string {
	return path + ":" + fmt.Sprintf("%d-%d", start, end)
}

func derefSlice(ptr *[]string) []string {
	if ptr == nil {
		return nil
	}
	return *ptr
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// meiliChunkDocument represents a chunk stored in Meilisearch.
type meiliChunkDocument struct {
	ID          string `json:"id"`
	FilePath    string `json:"file_path"`
	StartLine   int    `json:"start_line"`
	EndLine     int    `json:"end_line"`
	Content     string `json:"content"`
	ContentHash string `json:"content_hash"`
}
