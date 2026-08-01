package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
)

// SchedulerWorker checks every ~1 second for scheduled campaigns whose
// time_to_start has arrived and starts them by creating outbox messages.
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
			log.Printf("SchedulerWorker: Starting scheduled campaign %s (name=%q, time_to_start=%v)", c.ID, c.Name, c.TimeToStart)
			err := s.repo.StartCampaign(checkCtx, c.ID)
			if err != nil {
				log.Printf("SchedulerWorker: failed to start campaign %s: %v", c.ID, err)
				continue
			}
			log.Printf("SchedulerWorker: Successfully started campaign %s", c.ID)
		}
	}
}
