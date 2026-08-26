# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build with vendor for reproducible builds
COPY vendor ./vendor
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o orchestrator ./cmd/orchestrator

# Stage 2: Runner
FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/orchestrator .
COPY --from=builder /app/db/migrations ./db/migrations

EXPOSE 8080
CMD ["./orchestrator"]
