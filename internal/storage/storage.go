package storage

import (
	"context"
	"io"
)

type AttachmentStore interface {
	Put(ctx context.Context, key string, body io.Reader, contentType string) (string, error)
}
