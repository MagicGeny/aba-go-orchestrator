// Package logging is a minimal initialisation layer that makes the *existing*
// standard-library logger (log.Printf / log.Println / log.Fatalf, which this
// service already uses everywhere) write to two destinations at once:
//
//  1. stderr — the current console behaviour, unchanged and byte-for-byte
//     identical (Go's default "2006/01/02 15:04:05 message" format).
//  2. a rotating JSON-lines file (logs/orchestrator.log by default) that is
//     suitable for post-mortem incident analysis.
//
// The design goal is "logging only": no logging framework is introduced, no
// existing log call has to be rewritten, and no business logic is touched.
// Because the standard logger is re-pointed here, every pre-existing
// log.Printf(...) line automatically ends up in the rotating file too.
//
// In addition the package exposes a tiny structured-event helper
// (Info/Warn/Error) used by the orchestrator <-> RabbitMQ <-> worker
// diagnostics. Those events are written as flat JSON objects (all correlation
// identifiers become top-level fields, so `jq` can index them directly).
package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	// LogFileEnvVar overrides DefaultLogFile. Never hardcode an absolute path.
	LogFileEnvVar = "ORCHESTRATOR_LOG_FILE"

	// DefaultLogFile is relative on purpose so the service works from any
	// working directory / container layout.
	DefaultLogFile = "logs/orchestrator.log"

	// Rotation policy mirrors the MAX worker (aba-mx-adapter):
	// 20 MB per file, at most 5 rotated files, and no age-based deletion
	// (MaxAge == 0 means "never delete because of age").
	MaxSizeMB  = 20
	MaxBackups = 5

	// consoleTimeLayout reproduces Go's default log.LstdFlags output exactly.
	consoleTimeLayout = "2006/01/02 15:04:05"
	// fileTimeLayout is RFC3339 with milliseconds in UTC (same convention as
	// the worker's JSON logger, which emits "<iso>Z").
	fileTimeLayout = "2006-01-02T15:04:05.000Z07:00"
)

// Severity is the structured severity of a diagnostic event.
type Severity string

const (
	SeverityInfo  Severity = "INFO"
	SeverityWarn  Severity = "WARN"
	SeverityError Severity = "ERROR"
)

// sink implements io.Writer for the standard logger and additionally exposes
// structured event emission. Both paths share one mutex so concurrent writes
// from many goroutines never interleave inside a single line. The underlying
// lumberjack writer is itself safe for concurrent use.
type sink struct {
	mu      sync.Mutex
	console io.Writer
	file    io.Writer
	clock   func() time.Time
}

// Write is the io.Writer implementation used by the standard log package for
// legacy (unstructured) log.Printf / log.Println / log.Fatalf calls.
func (s *sink) Write(p []byte) (int, error) {
	// The log package already appended a trailing newline; drop it because
	// both destinations add their own line terminator.
	msg := strings.TrimRight(string(p), "\r\n")
	s.emit(SeverityInfo, "", msg, nil)
	return len(p), nil
}

// emit writes one record to the console and (when configured) to the rotating
// file. It never returns an error and never panics: logging must not be able
// to break the caller's control flow (e.g. inside a DB transition).
func (s *sink) emit(sev Severity, event, msg string, fields map[string]any) {
	now := time.Now()
	if s.clock != nil {
		now = s.clock()
	}

	// Console keeps the classic format. For structured events the payload is
	// emitted as flat JSON so it is both human-scannable and machine-parsable.
	consolePayload := msg
	if event != "" {
		record := make(map[string]any, len(fields)+1)
		for k, v := range fields {
			record[k] = v
		}
		record["event"] = event
		if line, err := json.Marshal(record); err == nil {
			consolePayload = string(line)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, _ = fmt.Fprintf(s.console, "%s %s\n", now.Format(consoleTimeLayout), consolePayload)

	if s.file == nil {
		return
	}

	record := make(map[string]any, len(fields)+5)
	for k, v := range fields {
		record[k] = v
	}
	record["timestamp"] = now.UTC().Format(fileTimeLayout)
	record["level"] = string(sev)
	record["message"] = msg
	if event != "" {
		record["event"] = event
	}
	record["pid"] = os.Getpid()

	line, err := json.Marshal(record)
	if err != nil {
		// Extremely unlikely (all values come from JSON-marshalable types);
		// fall back to a plain line so the record is not silently lost.
		line = []byte(fmt.Sprintf(`{"timestamp":%q,"level":%q,"message":%q}`,
			now.UTC().Format(fileTimeLayout), string(sev), msg))
	}
	_, _ = s.file.Write(append(line, '\n'))
}

var (
	initOnce sync.Once
	global   *sink
	closer   io.Closer
	initErr  error
	initPath string
)

// Init configures the process-wide standard logger to write to stderr AND to
// the rotating log file. It is idempotent and should be called before any
// other log call (immediately after .env loading). The returned error means
// file logging could not be enabled; console logging is then left untouched
// and the caller should keep running (file logging is diagnostics only, never
// a hard dependency).
func Init() error {
	initOnce.Do(func() {
		path := strings.TrimSpace(os.Getenv(LogFileEnvVar))
		if path == "" {
			path = DefaultLogFile
		}
		s, c, err := newSink(os.Stderr, path)
		if err != nil {
			initErr = err
			return
		}
		global = s
		closer = c
		initPath = path

		// Take ownership of the standard logger. Flags/prefix are cleared
		// because the sink re-creates the exact classic timestamp itself.
		log.SetFlags(0)
		log.SetPrefix("")
		log.SetOutput(s)

		Info("LOGGING_INITIALIZED", map[string]any{
			"log_file":      path,
			"console":       "stderr",
			"max_size_mb":   MaxSizeMB,
			"max_backups":   MaxBackups,
			"rotate_by_age": false,
		})
	})
	return initErr
}

// newSink builds the dual writer and the rotating file. Kept separate from
// Init so it can be unit tested without touching the global logger.
func newSink(console io.Writer, path string) (*sink, io.Closer, error) {
	if path == "" {
		path = DefaultLogFile
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, fmt.Errorf("create log directory %q: %w", dir, err)
		}
	}

	rotator := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    MaxSizeMB,  // megabytes before rotation
		MaxBackups: MaxBackups, // number of rotated files to retain
		MaxAge:     0,          // never delete rotated files because of age
		Compress:   false,
		LocalTime:  false, // name rotated files using UTC timestamps
	}
	return &sink{console: console, file: rotator}, rotator, nil
}

// Enabled reports whether file logging was successfully initialised.
func Enabled() bool {
	return global != nil && global.file != nil
}

// Path returns the effective log file path (empty when Init was not called).
func Path() string { return initPath }

// Close closes the rotating file cleanly. Safe to call multiple times; the
// orchestrator calls it from the graceful shutdown path.
func Close() error {
	if closer == nil {
		return nil
	}
	err := closer.Close()
	closer = nil
	return err
}

// Info emits a structured INFO event. A flat JSON object with `event` plus all
// provided fields is written to the console, and a JSON-lines record with
// timestamp/level/event fields is written to the rotating file.
func Info(event string, fields map[string]any) { emitEvent(SeverityInfo, event, fields) }

// Warn emits a structured WARN event.
func Warn(event string, fields map[string]any) { emitEvent(SeverityWarn, event, fields) }

// Error emits a structured ERROR event.
func Error(event string, fields map[string]any) { emitEvent(SeverityError, event, fields) }

func emitEvent(sev Severity, event string, fields map[string]any) {
	if global == nil {
		// Init was not called (e.g. a unit test): keep the event visible via
		// the standard logger instead of dropping it.
		record := make(map[string]any, len(fields)+2)
		for k, v := range fields {
			record[k] = v
		}
		record["event"] = event
		record["level"] = string(sev)
		if line, err := json.Marshal(record); err == nil {
			log.Printf("%s", line)
		}
		return
	}
	global.emit(sev, event, event, fields)
}

// Fields builds a field map from alternating key/value pairs for use with
// Info/Warn/Error. Empty values (nil, empty string, empty slice/map) are
// dropped so diagnostics stay compact and `jq`-friendly; booleans and zero
// numbers are preserved because they carry meaning (e.g. use_chat_id=false).
// Unpaired trailing keys are ignored.
func Fields(kv ...any) map[string]any {
	out := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok || key == "" {
			continue
		}
		if isEmptyValue(kv[i+1]) {
			continue
		}
		out[key] = kv[i+1]
	}
	return out
}

// WithFields returns a copy of base with the given alternating key/value pairs
// merged in (same empty-value rules as Fields). Used to extend a shared
// correlation set for the "finished"/"failed" variants of one event.
func WithFields(base map[string]any, kv ...any) map[string]any {
	out := make(map[string]any, len(base)+len(kv)/2)
	for k, v := range base {
		out[k] = v
	}
	for k, v := range Fields(kv...) {
		out[k] = v
	}
	return out
}

func isEmptyValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(t) == ""
	case []string:
		return len(t) == 0
	case []any:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return false
}

// StrDeref returns the pointed-to string, or "" for a nil pointer. Handy for
// optional worker payload fields (e.g. *error_message).
func StrDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// MaskPhone keeps just enough of a phone number to recognise it in an incident
// timeline without persisting the full MSISDN: the first three and the last
// four digits are kept ("915***0933"). The worker contract keeps sending the
// full number; only the log output is masked.
func MaskPhone(phone string) string {
	p := strings.TrimSpace(phone)
	if p == "" {
		return ""
	}
	r := []rune(p)
	const keepFirst, keepLast = 3, 4
	if len(r) <= 4 {
		return strings.Repeat("*", len(r))
	}
	first := keepFirst
	if len(r) <= first+keepLast {
		first = 1
	}
	if len(r) <= first+keepLast {
		return strings.Repeat("*", len(r))
	}
	masked := len(r) - first - keepLast
	return string(r[:first]) + strings.Repeat("*", masked) + string(r[len(r)-keepLast:])
}

// RedactURL removes credentials from a connection/URL so it can be safely
// written to the log file (e.g. amqp://***:***@rabbit:5672/).
func RedactURL(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return s
	}
	schemeEnd := strings.Index(s, "//")
	if schemeEnd < 0 || at < schemeEnd+2 {
		return s
	}
	prefix := s[:schemeEnd+2]
	host := s[at+1:]
	userinfo := s[schemeEnd+2 : at]
	if userinfo == "" {
		return s
	}
	if strings.Contains(userinfo, ":") {
		return prefix + "***:***@" + host
	}
	return prefix + "***@" + host
}
