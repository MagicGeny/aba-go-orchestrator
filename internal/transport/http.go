package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/MagicGeny/aba-go-orchestrator/internal/usecase"
	"github.com/MagicGeny/aba-go-orchestrator/internal/worker"
	"github.com/google/uuid"
)

type HTTPHandler struct {
	campaignUC   *usecase.CampaignUseCase
	hub          *Hub
	replyPoller  *worker.ReplyPoller
}

func NewHTTPHandler(campaignUC *usecase.CampaignUseCase, hub *Hub, replyPoller *worker.ReplyPoller) *HTTPHandler {
	return &HTTPHandler{
		campaignUC:  campaignUC,
		hub:         hub,
		replyPoller: replyPoller,
	}
}

func (h *HTTPHandler) UploadCampaign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := r.ParseMultipartForm(32 << 20) // 32MB
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tenantIDStr := r.FormValue("tenant_id")
	tenantID, _ := uuid.Parse(tenantIDStr)
	name := r.FormValue("name")
	template := r.FormValue("template")

	file, header, err := r.FormFile("excel")
	if err != nil {
		http.Error(w, "excel file is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Create independent context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	campaignID, err := h.campaignUC.UploadCampaign(ctx, tenantID, name, template, file, header.Filename)
	if err != nil {
		log.Printf("UploadCampaign error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"id": campaignID})
}

func (h *HTTPHandler) WorkerCallback(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		TaskID       uuid.UUID `json:"task_id"`
		Status       string    `json:"status"`
		ErrorMessage string    `json:"error_message"`
		SentAt       string    `json:"sent_at"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var sentAt *time.Time
	if payload.SentAt != "" {
		t, err := time.Parse(time.RFC3339, payload.SentAt)
		if err == nil {
			sentAt = &t
		}
	}

	// Create independent context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	campaign, err := h.campaignUC.UpdateTargetStatus(ctx, payload.TaskID, domain.TaskStatus(payload.Status), payload.ErrorMessage, sentAt)
	if err != nil {
		log.Printf("UpdateTargetStatus error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Broadcast update via WS
	h.hub.BroadcastStatus(campaign.ID, map[string]any{
		"campaign_id":     campaign.ID,
		"status":          campaign.Status,
		"processed_count": campaign.ProcessedCount,
		"total_count":     campaign.TotalCount,
		"error_count":     campaign.ErrorCount,
		// "replied_count":  ... (we'll need to count targets with replied status or add column)
	})

	w.WriteHeader(http.StatusOK)
}

func (h *HTTPHandler) RepliesWebhook(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		CampaignID uuid.UUID `json:"campaign_id"`
		Replies    []struct {
			Phone     string `json:"phone"`
			ReplyText string `json:"reply_text"`
			RepliedAt string `json:"replied_at"`
		} `json:"replies"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create independent context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var lastCampaign *domain.Campaign
	for _, reply := range payload.Replies {
		campaign, err := h.campaignUC.RegisterReply(ctx, payload.CampaignID, reply.Phone, reply.ReplyText, reply.RepliedAt)
		if err != nil {
			// Log error but continue with others
			log.Printf("RegisterReply error for phone %s: %v", reply.Phone, err)
			continue
		}
		lastCampaign = campaign
	}

	if lastCampaign != nil {
		// Broadcast update via WS
		h.hub.BroadcastStatus(lastCampaign.ID, map[string]any{
			"campaign_id":     lastCampaign.ID,
			"status":          lastCampaign.Status,
			"processed_count": lastCampaign.ProcessedCount,
			"total_count":     lastCampaign.TotalCount,
			"error_count":     lastCampaign.ErrorCount,
		})
	}

	w.WriteHeader(http.StatusOK)
}

func (h *HTTPHandler) ListCampaigns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tenantIDStr := r.URL.Query().Get("tenant_id")
	if tenantIDStr == "" {
		// Use the test tenant ID as default
		tenantIDStr = "00000000-0000-0000-0000-000000000000"
	}

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		http.Error(w, "invalid tenant_id", http.StatusBadRequest)
		return
	}

	// Create independent context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	campaigns, err := h.campaignUC.ListCampaigns(ctx, tenantID)
	if err != nil {
		log.Printf("Failed to list campaigns: %v", err)
		http.Error(w, "failed to list campaigns", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(campaigns)
}

func (h *HTTPHandler) TriggerPolling(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Create a new background context so it doesn't get canceled when request ends
	go h.replyPoller.PollActiveCampaigns(context.Background())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "ok", "message": "Polling triggered"})
}

func (h *HTTPHandler) DownloadCampaign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get campaign_id from query params
	campaignIDStr := r.URL.Query().Get("campaign_id")
	if campaignIDStr == "" {
		http.Error(w, "campaign_id is required", http.StatusBadRequest)
		return
	}

	campaignID, err := uuid.Parse(campaignIDStr)
	if err != nil {
		http.Error(w, "invalid campaign_id", http.StatusBadRequest)
		return
	}

	// Create independent context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Get the campaign first to get original filename
	campaign, err := h.campaignUC.GetCampaign(ctx, campaignID)
	if err != nil {
		log.Printf("Failed to get campaign: %v", err)
		http.Error(w, "campaign not found", http.StatusNotFound)
		return
	}

	// Use existing processed Excel file if available, otherwise generate new
	var filePath string
	if campaign.ProcessedExcelPath != nil && *campaign.ProcessedExcelPath != "" {
		filePath = *campaign.ProcessedExcelPath
	} else {
		// Generate Excel file if not exists
		filePath, err = h.campaignUC.GenerateExcel(ctx, campaignID)
		if err != nil {
			log.Printf("Failed to generate excel: %v", err)
			http.Error(w, "failed to generate excel", http.StatusInternalServerError)
			return
		}
	}

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Failed to open excel file: %v", err)
		http.Error(w, "failed to read excel file", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		log.Printf("Failed to get file info: %v", err)
		http.Error(w, "failed to read excel file", http.StatusInternalServerError)
		return
	}

	// Set headers for file download
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	// Build filename: original name without extension + "_status" + extension
	filename := campaign.OriginalExcelName
	if ext := getExtension(filename); ext != "" {
		filename = filename[:len(filename)-len(ext)] + "_status" + ext
	} else {
		filename += "_status.xlsx"
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileInfo.Size()))

	// Write file
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, file)
}

// Helper function to get file extension
func getExtension(filename string) string {
	for i := len(filename) - 1; i >= 0 && filename[i] != '.'; i-- {
		if filename[i] == '/' || filename[i] == '\\' {
			break
		}
		if i == 0 {
			return "" // No dot
		}
	}
	// Find the last dot
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] == '.' {
			return filename[i:]
		}
	}
	return ""
}
