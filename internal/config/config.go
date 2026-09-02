package config

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	_ "time/tzdata"
)

type Config struct {
	LimitColdMin           int
	LimitColdMax           int
	LimitColdEstimatedAvg  int
	LimitWarmDaily         int
	WorkWindowStart        string
	WorkWindowEnd          string
	IntervalColdMinMinutes int
	IntervalColdMaxMinutes int
	WorkWindowStartHour    int
	WorkWindowStartMinute  int
	WorkWindowEndHour      int
	WorkWindowEndMinute    int
	Location               *time.Location
	DisableAutoPolling     bool
}

func LoadFromEnv() Config {
	cfg := Config{
		LimitColdMin:           envInt("LIMIT_COLD_MIN", 100),
		LimitColdMax:           envInt("LIMIT_COLD_MAX", 300),
		LimitColdEstimatedAvg:  envInt("LIMIT_COLD_ESTIMATED_AVG", 150),
		LimitWarmDaily:         envInt("LIMIT_WARM_DAILY", 500),
		WorkWindowStart:        envString("WORK_WINDOW_START", "08:30"),
		WorkWindowEnd:          envString("WORK_WINDOW_END", "20:45"),
		IntervalColdMinMinutes: envInt("INTERVAL_COLD_MIN_MINUTES", 1),
		IntervalColdMaxMinutes: envInt("INTERVAL_COLD_MAX_MINUTES", 5),
		Location:               loadLocation(),
		DisableAutoPolling:     envBool("DISABLE_AUTO_POLLING", true),
	}
	parseWorkWindow(&cfg)
	log.Printf("config: timezone=%s work_window=%s-%s cold_interval=%d-%d min",
		cfg.Location.String(), cfg.WorkWindowStart, cfg.WorkWindowEnd,
		cfg.IntervalColdMinMinutes, cfg.IntervalColdMaxMinutes)
	return cfg
}

func (c Config) IsWithinWorkWindow(now time.Time) bool {
	local := now.In(c.Location)
	start := time.Date(local.Year(), local.Month(), local.Day(), c.WorkWindowStartHour, c.WorkWindowStartMinute, 0, 0, c.Location)
	end := time.Date(local.Year(), local.Month(), local.Day(), c.WorkWindowEndHour, c.WorkWindowEndMinute, 0, 0, c.Location)
	return !local.Before(start) && local.Before(end)
}

func (c Config) RandomColdInterval() time.Duration {
	min := c.IntervalColdMinMinutes
	max := c.IntervalColdMaxMinutes
	if max < min {
		max = min
	}
	minutes := min
	if max > min {
		minutes = min + rand.Intn(max-min+1)
	}
	return time.Duration(minutes) * time.Minute
}

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func loadLocation() *time.Location {
	tz := strings.TrimSpace(os.Getenv("TZ"))
	if tz == "" {
		tz = "Europe/Moscow"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		log.Printf("config: failed to load timezone %q (%v); using fixed UTC+3 (Moscow)", tz, err)
		return time.FixedZone("Europe/Moscow", 3*60*60)
	}
	return loc
}

func parseWorkWindow(cfg *Config) {
	cfg.WorkWindowStartHour, cfg.WorkWindowStartMinute = parseHHMM(cfg.WorkWindowStart, 7, 0)
	cfg.WorkWindowEndHour, cfg.WorkWindowEndMinute = parseHHMM(cfg.WorkWindowEnd, 21, 0)
}

func parseHHMM(raw string, defaultHour, defaultMinute int) (int, int) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return defaultHour, defaultMinute
	}
	h, errH := strconv.Atoi(strings.TrimSpace(parts[0]))
	m, errM := strconv.Atoi(strings.TrimSpace(parts[1]))
	if errH != nil || errM != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return defaultHour, defaultMinute
	}
	return h, m
}
