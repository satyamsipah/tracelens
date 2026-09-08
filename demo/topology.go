package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// role describes one demo service: where it listens and who it calls.
//
// The four services live in ONE binary selected by -service. That is not
// laziness: the call graph is the interesting part of a demo workload, and
// keeping it in a single table makes it readable and impossible for the four
// copies to drift apart. Compose still runs four separate containers, so the
// traces are genuinely cross-process.
type role struct {
	port       int
	downstream []string
	// work is the leaf operation name a service reports when it has no
	// downstream, standing in for a database or third-party call.
	work string
}

var topology = map[string]role{
	"gateway": {
		port:       8080,
		downstream: []string{"http://checkout:8081/work"},
	},
	"checkout": {
		port:       8081,
		downstream: []string{"http://inventory:8082/work", "http://payments:8083/work"},
	},
	"inventory": {
		port: 8082,
		work: "SELECT stock_levels",
	},
	"payments": {
		port: 8083,
		work: "POST psp.authorize",
	},
}

// settings are the per-service injectable failure knobs.
type settings struct {
	service    string
	port       int
	downstream []string
	work       string

	baseLatency time.Duration
	jitter      time.Duration
	errorRate   float64

	// driveRPS makes a service self-drive its own downstream calls, so that
	// `docker compose up` produces end-to-end traffic with no extra step.
	driveRPS float64
}

func loadSettings(service string) settings {
	r, ok := topology[service]
	if !ok {
		r = role{port: 8080, work: "noop"}
	}

	s := settings{
		service:     service,
		port:        envInt("DEMO_PORT", r.port),
		downstream:  r.downstream,
		work:        r.work,
		baseLatency: time.Duration(envInt("DEMO_LATENCY_MS", 10)) * time.Millisecond,
		jitter:      time.Duration(envInt("DEMO_JITTER_MS", 25)) * time.Millisecond,
		errorRate:   envFloat("DEMO_ERROR_RATE", 0.02),
		driveRPS:    envFloat("DEMO_DRIVE_RPS", 0),
	}
	if v := os.Getenv("DEMO_DOWNSTREAM"); v != "" {
		s.downstream = splitList(v)
	}
	if s.work == "" {
		s.work = "handle"
	}
	return s
}

func splitList(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
