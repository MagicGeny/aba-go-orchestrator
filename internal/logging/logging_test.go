package logging

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMaskPhone(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"9159570933", "915***0933"},
		{"79159570933", "791****0933"},
		{"1234", "****"},
		{"1234567", "1**4567"},
		{"abc", "***"},
		{" 9159570933 ", "915***0933"},
	}
	for _, c := range cases {
		if got := MaskPhone(c.in); got != c.want {
			t.Errorf("MaskPhone(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestMaskPhoneNeverLeaksFullNumber(t *testing.T) {
	const phone = "9159570933"
	masked := MaskPhone(phone)
	if strings.Contains(masked, phone) {
		t.Fatalf("masked phone %q still contains the full number", masked)
	}
	if !strings.Contains(masked, "*") {
		t.Fatalf("masked phone %q contains no mask character", masked)
	}
}

func TestRedactURL(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"amqp://guest:guest@localhost:5672/", "amqp://***:***@localhost:5672/"},
		{"amqp://user@host/", "amqp://***@host/"},
		{"amqp://localhost:5672/", "amqp://localhost:5672/"},
		{"no-at-sign", "no-at-sign"},
	}
	for _, c := range cases {
		if got := RedactURL(c.in); got != c.want {
			t.Errorf("RedactURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFieldsDropsEmptyButKeepsMeaningfulZeroValues(t *testing.T) {
	got := Fields(
		"task_id", "t-1",
		"empty", "",
		"missing", nil,
		"use_chat_id", false,
		"attempt", 0,
		"dangling",
	)
	if len(got) != 3 {
		t.Fatalf("want 3 fields, got %d: %#v", len(got), got)
	}
	if v, ok := got["use_chat_id"]; !ok || v != false {
		t.Fatalf("use_chat_id=false must be preserved, got %#v", got["use_chat_id"])
	}
	if v, ok := got["attempt"]; !ok || v != 0 {
		t.Fatalf("attempt=0 must be preserved, got %#v", got["attempt"])
	}
	if _, ok := got["empty"]; ok {
		t.Fatalf("empty string must be dropped")
	}
}

// TestSinkPreservesConsoleFormatAndWritesJSONLines is the core guarantee:
// legacy log lines keep the classic console format while the file receives one
// JSON object per line.
func TestSinkPreservesConsoleFormatAndWritesJSONLines(t *testing.T) {
	var console, file bytes.Buffer
	s, closer, err := newSink(&console, filepath.Join(t.TempDir(), "logs", "orchestrator.log"))
	if err != nil {
		t.Fatalf("newSink: %v", err)
	}
	defer closer.Close()

	// Redirect the rotating file to the in-memory buffer so the assertion can
	// read exactly what would be persisted.
	s.file = &file
	s.clock = func() time.Time { return time.Date(2026, 9, 22, 10, 0, 0, 123e6, time.UTC) }

	if _, err := s.Write([]byte("legacy line\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	s.emit(SeverityError, "WORKER_TASK_PUBLISH_FAILED", "WORKER_TASK_PUBLISH_FAILED", Fields(
		"task_id", "11111111-1111-1111-1111-111111111111",
		"error", "channel closed",
	))

	consoleLines := strings.Split(strings.TrimRight(console.String(), "\n"), "\n")
	if len(consoleLines) != 2 {
		t.Fatalf("want 2 console lines, got %d: %q", len(consoleLines), console.String())
	}
	if !strings.HasPrefix(consoleLines[0], "2026/09/22 10:00:00 legacy line") {
		t.Fatalf("legacy console line lost its classic format: %q", consoleLines[0])
	}
	if !strings.HasPrefix(consoleLines[1], "2026/09/22 10:00:00 {") {
		t.Fatalf("structured console line should keep the classic prefix: %q", consoleLines[1])
	}

	fileLines := strings.Split(strings.TrimRight(file.String(), "\n"), "\n")
	if len(fileLines) != 2 {
		t.Fatalf("want 2 file lines, got %d: %q", len(fileLines), file.String())
	}

	var legacy map[string]any
	if err := json.Unmarshal([]byte(fileLines[0]), &legacy); err != nil {
		t.Fatalf("legacy file line is not JSON: %v (%q)", err, fileLines[0])
	}
	for _, key := range []string{"timestamp", "level", "message", "pid"} {
		if _, ok := legacy[key]; !ok {
			t.Fatalf("legacy file record missing %q: %q", key, fileLines[0])
		}
	}
	if legacy["message"] != "legacy line" {
		t.Fatalf("legacy message = %v", legacy["message"])
	}
	if legacy["timestamp"] != "2026-09-22T10:00:00.123Z" {
		t.Fatalf("timestamp = %v", legacy["timestamp"])
	}

	var event map[string]any
	if err := json.Unmarshal([]byte(fileLines[1]), &event); err != nil {
		t.Fatalf("event file line is not JSON: %v (%q)", err, fileLines[1])
	}
	if event["event"] != "WORKER_TASK_PUBLISH_FAILED" {
		t.Fatalf("event = %v", event["event"])
	}
	if event["level"] != string(SeverityError) {
		t.Fatalf("level = %v", event["level"])
	}
	if event["task_id"] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("correlation id flattened incorrectly: %v", event["task_id"])
	}
	if event["message"] != "WORKER_TASK_PUBLISH_FAILED" {
		t.Fatalf("message = %v", event["message"])
	}
}

func TestNewSinkCreatesLogDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "logs", "orchestrator.log")
	s, closer, err := newSink(&bytes.Buffer{}, path)
	if err != nil {
		t.Fatalf("newSink: %v", err)
	}
	defer closer.Close()
	if _, err := s.Write([]byte("hello\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(data), `"message":"hello"`) {
		t.Fatalf("unexpected log file content: %q", string(data))
	}
	if MaxSizeMB != 20 || MaxBackups != 5 {
		t.Fatalf("rotation policy changed: maxSize=%dMB maxBackups=%d", MaxSizeMB, MaxBackups)
	}
}

// TestConcurrentWritesStayLineDelimited verifies that concurrent logging never
// interleaves two records inside one line.
func TestConcurrentWritesStayLineDelimited(t *testing.T) {
	var file bytes.Buffer
	s, closer, err := newSink(nopWriter{}, filepath.Join(t.TempDir(), "orchestrator.log"))
	if err != nil {
		t.Fatalf("newSink: %v", err)
	}
	defer closer.Close()
	s.file = &file

	const workers, perWorker = 8, 50
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				_, _ = s.Write([]byte("concurrent line"))
			}
		}()
	}
	wg.Wait()

	lines := strings.Split(strings.TrimRight(file.String(), "\n"), "\n")
	if len(lines) != workers*perWorker {
		t.Fatalf("want %d lines, got %d", workers*perWorker, len(lines))
	}
	for _, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("interleaved/corrupt line: %v (%q)", err, line)
		}
	}
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
