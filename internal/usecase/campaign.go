package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/MagicGeny/aba-go-orchestrator/internal/config"
	"github.com/MagicGeny/aba-go-orchestrator/internal/domain"
	"github.com/MagicGeny/aba-go-orchestrator/internal/storage"
	"github.com/google/uuid"
	"github.com/xuri/excelize/v2"
)

const uploadDir = "uploads"

type CampaignUseCase struct {
	repo            domain.CampaignRepository
	attachmentStore storage.AttachmentStore
	cfg             config.Config
}

func NewCampaignUseCase(repo domain.CampaignRepository, attachmentStore storage.AttachmentStore, cfg config.Config) *CampaignUseCase {
	return &CampaignUseCase{repo: repo, attachmentStore: attachmentStore, cfg: cfg}
}

func (uc *CampaignUseCase) GetCampaign(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	return uc.repo.GetCampaign(ctx, id)
}

func (uc *CampaignUseCase) ListCampaigns(ctx context.Context, tenantID uuid.UUID) ([]*domain.Campaign, error) {
	return uc.repo.ListCampaigns(ctx, tenantID)
}

func (uc *CampaignUseCase) GetActiveCampaignsReadyToStart(ctx context.Context) ([]*domain.Campaign, error) {
	return uc.repo.GetActiveCampaignsReadyToStart(ctx)
}

func (uc *CampaignUseCase) StopCampaign(ctx context.Context, id uuid.UUID) error {
	return uc.repo.StopCampaign(ctx, id)
}

func (uc *CampaignUseCase) UpdateTargetStatus(ctx context.Context, targetID uuid.UUID, status domain.TaskStatus, lastError string, sentAt *time.Time) (*domain.Campaign, error) {
	var errPtr *string
	if lastError != "" {
		errPtr = &lastError
	}
	campaign, err := uc.repo.UpdateTargetStatus(ctx, targetID, status, errPtr, sentAt)
	if err != nil {
		return nil, err
	}
	return campaign, nil
}

func (uc *CampaignUseCase) RegisterReply(ctx context.Context, campaignID uuid.UUID, phone, text string, repliedAt string) (*domain.Campaign, error) {
	t, err := time.Parse(time.RFC3339, repliedAt)
	if err != nil {
		t = time.Now().UTC()
	}
	campaign, err := uc.repo.RegisterReply(ctx, campaignID, phone, text, t)
	if err != nil {
		return nil, err
	}
	return campaign, nil
}

type excelRow struct {
	index int
	name  string
	phone string
}

func generateSemanticFilename(campaignName string, createdAt time.Time, originalExt string) string {
	// Generate a hash for uniqueness
	hashInput := fmt.Sprintf("%s-%s-%s", campaignName, createdAt.Format(time.RFC3339), uuid.New().String())
	hash := sha256.Sum256([]byte(hashInput))
	shortHash := hex.EncodeToString(hash[:])[:8]

	// Sanitize campaign name: replace spaces with underscores, remove invalid chars
	sanitizedName := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, campaignName)

	// Format: имя_компании-дата_рассылки-хеш.расширение
	return fmt.Sprintf("%s-%s-%s%s",
		sanitizedName,
		createdAt.Format("20060102"),
		shortHash,
		originalExt)
}

func (uc *CampaignUseCase) UploadCampaign(ctx context.Context, tenantID uuid.UUID, name, template string, startImmediately bool, timeToStart *time.Time, excelReader io.Reader, originalName string, attachmentReader io.Reader, attachmentName string) (uuid.UUID, error) {
	// Ensure upload directory exists
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return uuid.Nil, fmt.Errorf("failed to create upload directory: %w", err)
	}

	// Create a temporary file to read from
	tempFile, err := os.CreateTemp(uploadDir, "temp-*.xlsx")
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write uploaded file to temp file
	_, err = io.Copy(tempFile, excelReader)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to write temp file: %w", err)
	}
	tempFile.Close()

	// Open the file with Excelize
	f, err := excelize.OpenFile(tempFile.Name())
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to open excel: %w", err)
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	rows, err := f.Rows(sheetName)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to get rows: %w", err)
	}
	defer rows.Close()

	campaignID, err := uuid.NewV7()
	if err != nil {
		campaignID = uuid.New()
	}

	createdAt := time.Now().UTC()
	originalExt := filepath.Ext(originalName)
	if originalExt == "" {
		originalExt = ".xlsx"
	}
	originalFilename := generateSemanticFilename(name, createdAt, originalExt)
	originalFilePath := filepath.Join(uploadDir, originalFilename)

	// Copy the temp file to the final original file
	err = copyFile(tempFile.Name(), originalFilePath)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to save original file: %w", err)
	}

	var attachmentURL *string
	var attachmentOriginalName *string
	if attachmentReader != nil {
		if uc.attachmentStore == nil {
			_ = os.Remove(originalFilePath)
			return uuid.Nil, fmt.Errorf("attachment storage is not configured")
		}

		ext := filepath.Ext(attachmentName)
		if ext == "" {
			ext = ".bin"
		}
		attachmentFilename := generateSemanticFilename(fmt.Sprintf("%s-attachment", name), createdAt, ext)
		objectKey := filepath.ToSlash(filepath.Join(campaignID.String(), attachmentFilename))

		url, err := uc.attachmentStore.Put(ctx, objectKey, attachmentReader, "application/octet-stream")
		if err != nil {
			_ = os.Remove(originalFilePath)
			return uuid.Nil, fmt.Errorf("failed to upload attachment: %w", err)
		}
		attachmentURL = &url
		if attachmentName != "" {
			attachmentOriginalName = &attachmentName
		}
	}

	// Set initial status based on start_immediately flag
	// If startImmediately is true, the campaign starts right away.
	// If false, it stays in draft and will be picked up by the poller when time_to_start arrives.
	status := domain.CampaignStatusDraft
	if startImmediately {
		status = domain.CampaignStatusProcessing
	}

	campaign := &domain.Campaign{
		ID:                 campaignID,
		TenantID:           tenantID,
		Name:               name,
		MessageTemplate:    template,
		Status:             status,
		OriginalExcelName:  originalName,
		OriginalExcelPath:  &originalFilePath,
		ProcessedExcelPath: nil, // Will be generated later
		AttachmentURL:      attachmentURL,
		AttachmentName:     attachmentOriginalName,
		Deleted:            false,
		StartImmediately:   startImmediately,
		TimeToStart:        timeToStart,
		CreatedAt:          createdAt,
		UpdatedAt:          createdAt,
	}

	jobs := make(chan excelRow, 100)
	results := make(chan *domain.CampaignTarget, 100)
	errChan := make(chan error, 1)

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU()

	// Start worker pool
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range jobs {
				// Normalize phone: keeping 10 digits starting with 9
				phone := domain.NormalizePhone(row.phone)
				if phone == "" {
					continue
				}

				targetID, err := uuid.NewV7()
				if err != nil {
					targetID = uuid.New()
				}

				target := &domain.CampaignTarget{
					ID:              targetID,
					CampaignID:      campaignID,
					ClientName:      row.name,
					PhoneNormalized: phone,
					MessengerType:   domain.DefaultMessengerType,
					ExcelRowIndex:   row.index,
					Status:          domain.TaskStatusPending,
				}
				results <- target
			}
		}()
	}

	// Read rows and send to jobs
	go func() {
		defer close(jobs)
		rowIndex := 0
		// Skip header
		if rows.Next() {
			rowIndex++
		}
		for rows.Next() {
			cols, err := rows.Columns()
			if err != nil {
				errChan <- err
				return
			}
			if len(cols) < 2 {
				continue
			}
			jobs <- excelRow{
				index: rowIndex,
				name:  cols[0],
				phone: cols[1],
			}
			rowIndex++
		}
	}()

	// Wait for workers in a separate goroutine
	go func() {
		wg.Wait()
		close(results)
	}()

	var targets []*domain.CampaignTarget

	for target := range results {
		targets = append(targets, target)
	}

	select {
	case err := <-errChan:
		return uuid.Nil, err
	default:
	}

	campaign.TotalCount = len(targets)

	phones := make([]string, 0, len(targets))
	for _, t := range targets {
		phones = append(phones, t.PhoneNormalized)
	}
	mappedCount, err := uc.repo.CountMappedPhones(ctx, tenantID, domain.DefaultMessengerType, phones)
	if err != nil {
		_ = os.Remove(originalFilePath)
		return uuid.Nil, fmt.Errorf("failed to count warm contacts: %w", err)
	}
	coldCount := len(targets) - mappedCount
	if coldCount < 0 {
		coldCount = 0
	}
	avg := uc.cfg.LimitColdEstimatedAvg
	if avg <= 0 {
		avg = 41
	}
	estimatedDays := (coldCount + avg - 1) / avg
	if estimatedDays < 1 {
		estimatedDays = 1
	}
	campaign.EstimatedDays = &estimatedDays
	startAt := createdAt
	if !startImmediately && timeToStart != nil {
		startAt = *timeToStart
	}
	completion := startAt.AddDate(0, 0, estimatedDays)
	campaign.ScheduledCompletionDate = &completion

	if err := uc.repo.CreateCampaign(ctx, campaign, targets, nil); err != nil {
		// Clean up the uploaded file if campaign creation fails
		_ = os.Remove(originalFilePath)
		return uuid.Nil, err
	}

	return campaignID, nil
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

/*
func normalizePhone(p string) string {
	// Simple normalization: find 10 digits starting with 9
	var digits []rune
	for _, r := range p {
		if r >= '0' && r <= '9' {
			digits = append(digits, r)
		}
	}

	s := string(digits)
	if len(s) > 10 {
		// If 11 digits and starts with 7 or 8, take last 10
		if (s[0] == '7' || s[0] == '8') && s[1] == '9' {
			return s[1:]
		}
	}
	if len(s) == 10 && s[0] == '9' {
		return s
	}
	return ""
}
*/

// GenerateExcel creates or updates an Excel file with current campaign status
func (uc *CampaignUseCase) GenerateExcel(ctx context.Context, campaignID uuid.UUID) (string, error) {
	log.Printf("GenerateExcel: Starting for campaign %s", campaignID)

	// Get campaign and targets
	campaign, err := uc.repo.GetCampaign(ctx, campaignID)
	if err != nil {
		log.Printf("GenerateExcel: failed to get campaign: %v", err)
		return "", fmt.Errorf("failed to get campaign: %w", err)
	}

	targets, err := uc.repo.GetCampaignTargets(ctx, campaignID)
	if err != nil {
		log.Printf("GenerateExcel: failed to get targets: %v", err)
		return "", fmt.Errorf("failed to get targets: %w", err)
	}
	log.Printf("GenerateExcel: Got %d targets", len(targets))

	// If no original file path, return error
	if campaign.OriginalExcelPath == nil || *campaign.OriginalExcelPath == "" {
		return "", fmt.Errorf("original excel file not found")
	}

	filePathToOpen := *campaign.OriginalExcelPath
	log.Printf("GenerateExcel: Using original file: %s", filePathToOpen)

	// Open the file
	f, err := excelize.OpenFile(filePathToOpen)
	if err != nil {
		log.Printf("GenerateExcel: failed to open excel file: %v", err)
		return "", fmt.Errorf("failed to open excel file: %w", err)
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	log.Printf("GenerateExcel: Using sheet name: %s", sheetName)

	rows, err := f.GetRows(sheetName)
	if err != nil {
		log.Printf("GenerateExcel: failed to get rows: %v", err)
		return "", fmt.Errorf("failed to get rows: %w", err)
	}
	log.Printf("GenerateExcel: Got %d rows from sheet", len(rows))

	targetMap := make(map[int]*domain.CampaignTarget)
	for _, target := range targets {
		targetMap[target.ExcelRowIndex] = target
		if target.LastReplyText != nil {
			log.Printf("GenerateExcel: Target index %d has reply: %s", target.ExcelRowIndex, *target.LastReplyText)
		}
	}

	statusCol := ""
	replyCol := ""
	replyTimeCol := ""
	headerRow := rows[0]
	for colIdx, colVal := range headerRow {
		colName, _ := excelize.ColumnNumberToName(colIdx + 1)
		if colVal == "Статус MAX отправки" {
			statusCol = colName
		} else if colVal == "Ответ пользователя" {
			replyCol = colName
		} else if colVal == "Время получения ответа" {
			replyTimeCol = colName
		}
	}
	log.Printf("GenerateExcel: Columns found: status=%s, reply=%s, replyTime=%s", statusCol, replyCol, replyTimeCol)

	lastCol := len(headerRow)
	if statusCol == "" {
		colName, _ := excelize.ColumnNumberToName(lastCol + 1)
		statusCol = colName
		cell, _ := excelize.JoinCellName(statusCol, 1)
		f.SetCellValue(sheetName, cell, "Статус MAX отправки")
		lastCol++
		log.Printf("GenerateExcel: Added status column at: %s", statusCol)
	}
	if replyCol == "" {
		colName, _ := excelize.ColumnNumberToName(lastCol + 1)
		replyCol = colName
		cell, _ := excelize.JoinCellName(replyCol, 1)
		f.SetCellValue(sheetName, cell, "Ответ пользователя")
		lastCol++
		log.Printf("GenerateExcel: Added reply column at: %s", replyCol)
	}
	if replyTimeCol == "" {
		colName, _ := excelize.ColumnNumberToName(lastCol + 1)
		replyTimeCol = colName
		cell, _ := excelize.JoinCellName(replyTimeCol, 1)
		f.SetCellValue(sheetName, cell, "Время получения ответа")
		log.Printf("GenerateExcel: Added reply time column at: %s", replyTimeCol)
	}

	for rowNum := 1; rowNum < len(rows); rowNum++ {
		targetIndex := rowNum
		target, ok := targetMap[targetIndex]

		var statusText string = "Не обработан"
		var replyText string = "не прочитано"
		var replyTimeText string = ""

		if ok {
			statusText = target.Status.StatusText()
			if target.Status == domain.TaskStatusUserNotFoundByPhone {
				messenger := target.MessengerType
				if messenger == "" {
					messenger = domain.DefaultMessengerType
				}
				statusText = fmt.Sprintf("Пользователь на найден в %s", messenger)
			} else if target.LastError != nil && target.Status == domain.TaskStatusFailed {
				statusText = fmt.Sprintf("%s: %s", statusText, *target.LastError)
			}

			if target.Status == domain.TaskStatusReplied {
				if target.LastReplyText != nil {
					replyText = *target.LastReplyText
					log.Printf("GenerateExcel: Row %d - setting reply text to: %s", rowNum, replyText)
				}
				if target.RepliedAt != nil {
					replyTimeText = target.RepliedAt.In(uc.cfg.Location).Format("2006-01-02 15:04:05")
				}
			} else if target.Status == domain.TaskStatusViewed {
				replyText = "ПРОЧИТАНО"
				// For viewed, use the last updated time as the time
				replyTimeText = target.UpdatedAt.In(uc.cfg.Location).Format("2006-01-02 15:04:05")
			}
		}

		statusCell, _ := excelize.JoinCellName(statusCol, rowNum+1)
		f.SetCellValue(sheetName, statusCell, statusText)

		replyCell, _ := excelize.JoinCellName(replyCol, rowNum+1)
		f.SetCellValue(sheetName, replyCell, replyText)

		replyTimeCell, _ := excelize.JoinCellName(replyTimeCol, rowNum+1)
		f.SetCellValue(sheetName, replyTimeCell, replyTimeText)
	}

	processedFilename := generateSemanticFilename(campaign.Name+"_processed_"+uuid.NewString(), time.Now().UTC(), ".xlsx")
	processedFilePath := filepath.Join(uploadDir, processedFilename)

	log.Printf("GenerateExcel: Saving processed file to: %s", processedFilePath)
	err = f.SaveAs(processedFilePath)
	if err != nil {
		log.Printf("GenerateExcel: failed to save processed excel: %v", err)
		return "", fmt.Errorf("failed to save processed excel: %w", err)
	}

	campaign.ProcessedExcelPath = &processedFilePath
	err = uc.repo.UpdateCampaign(ctx, campaign)
	if err != nil {
		fmt.Printf("Warning: failed to update campaign with processed file path: %v\n", err)
	}

	log.Printf("GenerateExcel: Done! Processed file saved at: %s", processedFilePath)
	return processedFilePath, nil
}
