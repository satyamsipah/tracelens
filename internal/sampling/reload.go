package sampling

import (
	"log/slog"
	"os"
	"time"

	"github.com/satyamsipah/tracelens/internal/observability"
)

// PolicyFileWatcher polls a policy YAML file's mtime and hot-swaps the
// Assembler's active chain on change.
//
// Polling rather than fsnotify: policy config is not a sub-second-latency
// need, and polling avoids one more dependency for marginal benefit. A parse
// or build failure on reload is logged and the PREVIOUS chain stays active --
// a bad edit must never silently disable sampling or crash the assembler.
type PolicyFileWatcher struct {
	path     string
	interval time.Duration
	log      *slog.Logger
	m        *observability.Metrics

	assembler *Assembler
	lastMod   time.Time
}

// NewPolicyFileWatcher builds a watcher. Call Run to start polling. Reloaded
// chains carry no metrics wiring (rate_limiting's gauge/counter stay unset)
// -- fine for tests. Use NewPolicyFileWatcherWithMetrics for a live deployment.
func NewPolicyFileWatcher(path string, interval time.Duration, a *Assembler, log *slog.Logger) *PolicyFileWatcher {
	return NewPolicyFileWatcherWithMetrics(path, interval, a, log, nil)
}

// NewPolicyFileWatcherWithMetrics is NewPolicyFileWatcher with metrics
// threaded into every reloaded chain.
func NewPolicyFileWatcherWithMetrics(path string, interval time.Duration, a *Assembler, log *slog.Logger, m *observability.Metrics) *PolicyFileWatcher {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &PolicyFileWatcher{path: path, interval: interval, assembler: a, log: log, m: m}
}

// Run polls until stop is closed. Intended to run in its own goroutine.
func (w *PolicyFileWatcher) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			w.checkAndReload()
		}
	}
}

func (w *PolicyFileWatcher) checkAndReload() {
	info, err := os.Stat(w.path)
	if err != nil {
		w.log.Warn("policy file stat failed, keeping current chain", slog.String("path", w.path), slog.String("error", err.Error()))
		return
	}
	if !info.ModTime().After(w.lastMod) {
		return
	}

	chain, err := LoadPolicyFileWithMetrics(w.path, w.m)
	if err != nil {
		w.log.Error("policy file reload failed, keeping current chain",
			slog.String("path", w.path), slog.String("error", err.Error()))
		return
	}

	w.lastMod = info.ModTime()
	w.assembler.SetChain(chain)
	w.log.Info("policy chain reloaded", slog.String("path", w.path))
}
