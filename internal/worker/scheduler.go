package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
)

// SchedulerWorker checks every ~2 seconds for scheduled campaigns whose
// time_to_start has arrived and transitions them to processing.
// Per-tenant dosing is handled by CampaignDoser.
type SchedulerWorker struct {
	repo      domain.CampaignRepository
	mu        sync.Mutex
	isRunning bool
}

func NewSchedulerWorker(repo domain.CampaignRepository) *SchedulerWorker {
	return &SchedulerWorker{
		repo: repo,
	}
}

func (s *SchedulerWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("SchedulerWorker: Started (checking every 1s for campaigns to start)")

	for {
		select {
		case <-ctx.Done():
			log.Println("SchedulerWorker: Stopped")
			return
		case <-ticker.C:
			s.checkAndStartCampaigns(ctx)
		}
	}
}

func (s *SchedulerWorker) checkAndStartCampaigns(ctx context.Context) {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return
	}
	s.isRunning = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()
	}()

	// Use a dedicated context with a short timeout so we don't block
	checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get campaigns that are ready to start (draft with time_to_start <= now, OR start_immediately=true)
	readyCampaigns, err := s.repo.GetActiveCampaignsReadyToStart(checkCtx)
	if err != nil {
		log.Printf("SchedulerWorker: failed to get ready campaigns: %v", err)
		return
	}

	for _, c := range readyCampaigns {
		if c.Status == domain.CampaignStatusDraft {
			campaignID := c.ID
			campaignName := c.Name
			campaignTime := c.TimeToStart
			log.Printf("SchedulerWorker: Starting scheduled campaign %s (name=%q, time_to_start=%v)", campaignID, campaignName, campaignTime)
			go func() {
				startCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
				defer cancel()
				err := s.repo.StartCampaign(startCtx, campaignID)
				if err != nil {
					log.Printf("SchedulerWorker: failed to start campaign %s: %v", campaignID, err)
					return
				}
				log.Printf("SchedulerWorker: Successfully started campaign %s", campaignID)
			}()
		}
	}
}
