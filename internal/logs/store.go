package logs

import (
	"strings"

	"github.com/satyamsipah/tracelens/internal/observability"
)

// TemplateUpsert is what TemplateStore reports whenever a template's text is
// new or has just changed (creation, or a later log widened it further), for
// the caller to persist into ClickHouse's dictionary table. Kept as a plain
// struct rather than a direct storage dependency: no ClickHouse SQL exists
// outside internal/storage, so this package hands back data, not an INSERT.
//
// Reporting on EVERY change, not just creation, matters: persisting only at
// creation would leave the dictionary holding the template's original,
// narrower literal text forever, once a later log causes it to generalize
// further -- log_templates is a ReplacingMergeTree keyed on template_id
// specifically so a repeat upsert with fresher text is the correct, cheap fix.
type TemplateUpsert struct {
	TemplateID uint32
	Text       string
}

// TemplateStore couples a Drain Tree with the storage-saving measurement the
// phase explicitly asks for: comparing what the raw log body would have cost
// against what template_id + params actually costs for the same line.
type TemplateStore struct {
	tree *Tree
	m    *observability.Metrics
}

// NewTemplateStore builds a store over an existing tree.
func NewTemplateStore(tree *Tree, m *observability.Metrics) *TemplateStore {
	return &TemplateStore{tree: tree, m: m}
}

// Process parses body and returns the resulting match, plus a non-nil
// TemplateUpsert whenever the template's text is new or just changed -- the
// caller is expected to persist that into tracelens.log_templates.
func (s *TemplateStore) Process(body string) (Match, *TemplateUpsert) {
	match := s.tree.Parse(body)

	if s.m != nil {
		s.m.LogBytesRaw.Add(float64(len(body)))
		s.m.LogBytesTemplated.Add(float64(templatedSize(match)))
	}

	if !match.Changed {
		return match, nil
	}
	return match, &TemplateUpsert{
		TemplateID: match.TemplateID,
		Text:       strings.Join(match.Template, " "),
	}
}

// templatedSize estimates the on-the-wire cost of template_id (a dense
// uint32) plus params, versus len(body) for the raw string it replaces. This
// is an estimate of SHAPE, not a promise of the exact compressed byte count
// ClickHouse will produce -- that number is measured separately, the same
// way phase 1 measured compression against real ClickHouse system tables
// rather than trusting an estimate.
func templatedSize(m Match) int {
	size := 4 // template_id, uint32
	for _, p := range m.Params {
		size += len(p) + 1 // +1 for an implicit separator between params
	}
	return size
}
