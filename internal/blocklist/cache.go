package blocklist

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/google/uuid"
)

type Cache struct {
	repo     domain.BlocklistRepository
	interval time.Duration
	data     atomic.Value
}

func NewCache(repo domain.BlocklistRepository, interval time.Duration) *Cache {
	c := &Cache{
		repo:     repo,
		interval: interval,
	}
	c.data.Store(map[string]struct{}{})
	return c
}

func (c *Cache) IsBlocked(tenantID uuid.UUID, phoneNormalized string) bool {
	m, _ := c.data.Load().(map[string]struct{})
	_, ok := m[key(tenantID, phoneNormalized)]
	return ok
}

func (c *Cache) Add(tenantID uuid.UUID, phoneNormalized string) {
	prev, _ := c.data.Load().(map[string]struct{})

	next := make(map[string]struct{}, len(prev)+1)
	for k := range prev {
		next[k] = struct{}{}
	}
	next[key(tenantID, phoneNormalized)] = struct{}{}
	c.data.Store(next)
}

func (c *Cache) Refresh(ctx context.Context) error {
	items, err := c.repo.ListBlockedRecipients(ctx)
	if err != nil {
		return err
	}

	next := make(map[string]struct{}, len(items))
	for _, it := range items {
		next[key(it.TenantID, it.PhoneNormalized)] = struct{}{}
	}
	c.data.Store(next)
	return nil
}

func (c *Cache) Run(ctx context.Context) {
	initialCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_ = c.Refresh(initialCtx)
	cancel()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := c.Refresh(refreshCtx)
			cancel()
			if err != nil {
				log.Printf("blocklist cache refresh failed: %v", err)
			}
		}
	}
}

func key(tenantID uuid.UUID, phoneNormalized string) string {
	return tenantID.String() + "|" + phoneNormalized
}
