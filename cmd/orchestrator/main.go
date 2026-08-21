package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"

	"github.com/MagicGeny/aba-go-orchestrator/internal/blocklist"
	"github.com/MagicGeny/aba-go-orchestrator/internal/config"
	"github.com/MagicGeny/aba-go-orchestrator/internal/repository"
	"github.com/MagicGeny/aba-go-orchestrator/internal/storage"
	"github.com/MagicGeny/aba-go-orchestrator/internal/transport"
	"github.com/MagicGeny/aba-go-orchestrator/internal/usecase"
	"github.com/MagicGeny/aba-go-orchestrator/internal/worker"
)

func main() {
	// Load .env from the current working directory, and from the executable's
	// directory. Both attempts are best-effort.
	_ = godotenv.Load()
	if exe, err := os.Executable(); err == nil {
		_ = godotenv.Load(filepath.Join(filepath.Dir(exe), ".env"))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Connect to Postgres
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/aba_orchestrator?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	defer pool.Close()

	// 1.1 AUTO MIGRATIONS
	log.Println("Running database migrations...")
	runMigrations(dbURL)

	// 2. Connect to RabbitMQ
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}
	amqpConn, err := amqp091.Dial(amqpURL)
	if err != nil {
		log.Fatalf("unable to connect to rabbitmq: %v", err)
	}
	defer amqpConn.Close()

	// 3. Initialize layers
	repo := repository.NewPostgresRepository(pool)
	var attachmentStore storage.AttachmentStore
	{
		endpoint := os.Getenv("SEAWEED_S3_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://127.0.0.1:8333"
		}
		bucket := os.Getenv("SEAWEED_S3_BUCKET")
		if bucket == "" {
			bucket = "campaign-attachments"
		}
		region := os.Getenv("SEAWEED_S3_REGION")
		accessKey := os.Getenv("SEAWEED_S3_ACCESS_KEY")
		secretKey := os.Getenv("SEAWEED_S3_SECRET_KEY")
		publicBaseURL := os.Getenv("SEAWEED_S3_PUBLIC_BASE_URL")
		submitURL := os.Getenv("SEAWEED_SUBMIT_URL")
		if submitURL == "" {
			// Default: SeaweedFS filer multipart upload endpoint (port 8888).
			// NOTE: do NOT default to 8080 — the orchestrator runs there.
			submitURL = "http://localhost:8888/" + bucket + "/"
		}
		httpPublic := os.Getenv("SEAWEED_HTTP_PUBLIC_BASE_URL")
		if httpPublic == "" {
			// Default to the filer's host:port (the filer serves both uploads
			// and downloads). This is derived from the submit URL so the two
			// are always in sync.
			httpPublic = deriveHost(submitURL)
		}
		if httpPublic == "" {
			httpPublic = endpoint
		}
		forceHTTP, _ := strconv.ParseBool(os.Getenv("SEAWEED_FORCE_HTTP_UPLOAD"))

		store, err := storage.NewS3Store(ctx, storage.S3StoreConfig{
			Endpoint:          endpoint,
			Region:            region,
			Bucket:            bucket,
			AccessKey:         accessKey,
			SecretKey:         secretKey,
			PublicBaseURL:     publicBaseURL,
			SeaweedSubmitURL:  submitURL,
			ForceHTTPUpload:   forceHTTP,
			HTTPPublicBaseURL: httpPublic,
		})
		if err != nil {
			log.Printf("attachment store init failed: %v", err)
		} else {
			log.Printf("attachment store: submit=%s public_base=%s s3_endpoint=%s forceHTTP=%v",
				submitURL, httpPublic, endpoint, forceHTTP)
			attachmentStore = store
		}
	}

	cfg := config.LoadFromEnv()
	campaignUC := usecase.NewCampaignUseCase(repo, attachmentStore, cfg)
	hub := transport.NewHub()

	// 4. Start Workers
	blocklistCache := blocklist.NewCache(repo, 10*time.Minute)
	go blocklistCache.Run(ctx)

	outboxWorker, err := worker.NewOutboxWorker(repo, amqpConn, worker.QueueSend, worker.QueueSendExistingChat, "tasks.messages.results_replies_queue", blocklistCache)
	if err != nil {
		log.Fatalf("failed to init outbox worker: %v", err)
	}
	go outboxWorker.Run(ctx)

	campaignDoser := worker.NewCampaignDoser(repo, cfg)
	go campaignDoser.Run(ctx)

	schedulerWorker := worker.NewSchedulerWorker(repo)
	go schedulerWorker.Run(ctx)

	replyPoller, err := worker.NewReplyPoller(repo, amqpConn, "tasks.messages.poll_replies")
	if err != nil {
		log.Fatalf("failed to init reply poller: %v", err)
	}
	go replyPoller.Run(ctx)

	resultConsumer, err := worker.NewResultConsumer(repo, campaignUC, amqpConn, "tasks.messages.results_replies_queue", blocklistCache)
	if err != nil {
		log.Fatalf("failed to init result consumer: %v", err)
	}
	go func() {
		if err := resultConsumer.Run(ctx); err != nil {
			log.Printf("result consumer error: %v", err)
		}
	}()

	handler := transport.NewHTTPHandler(campaignUC, hub, replyPoller)

	// 5. Setup Routes
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/campaigns/upload", handler.UploadCampaign)
	mux.HandleFunc("/api/v1/campaigns", handler.ListCampaigns)
	mux.HandleFunc("/api/v1/campaigns/", handler.CampaignSubroutes)
	mux.HandleFunc("/api/v1/campaigns/trigger-polling", handler.TriggerPolling)
	mux.HandleFunc("/api/v1/campaigns/stream", hub.HandleStream)
	mux.HandleFunc("/api/v1/workers/callback", handler.WorkerCallback)
	mux.HandleFunc("/api/v1/workers/replies-webhook", handler.RepliesWebhook)
	mux.HandleFunc("/api/v1/campaigns/download", handler.DownloadCampaign)
	mux.HandleFunc("/api/v1/campaigns/stop", handler.StopCampaign)

	// Add CORS middleware
	corsMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == "OPTIONS" {
			return
		}

		mux.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: corsMux,
	}

	// 6. Start HTTP Server
	go func() {
		log.Printf("Starting server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// 7. Graceful Shutdown
	<-ctx.Done()
	log.Println("Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exiting")
}

func runMigrations(dbURL string) {
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		log.Fatalf("could not open db for migrations: %v", err)
	}
	defer db.Close()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Fatalf("could not create database driver: %v", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://db/migrations",
		"aba", driver)
	if err != nil {
		log.Fatalf("migration init failed: %v", err)
	}

	// Накатываем все новые миграции до упора
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("failed to run migrations: %v", err)
	}

	log.Println("Migrations applied successfully (or already up to date)!")
}

// deriveHost extracts the scheme + host[:port] from a URL, returning an empty
// string if the URL is malformed. Used to default the public base URL to the
// same host:port as the SeaweedFS filer submit URL.
func deriveHost(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	// Trim any path after the host.
	for i := 0; i < len(rawURL); i++ {
		if rawURL[i] == '/' && i > 0 && rawURL[i-1] == '/' {
			// found "//"; the host begins at i+1
			rest := rawURL[i+2:]
			end := len(rest)
			for j := 0; j < len(rest); j++ {
				if rest[j] == '/' {
					end = j
					break
				}
			}
			return rawURL[:i+2] + rest[:end]
		}
	}
	return ""
}
