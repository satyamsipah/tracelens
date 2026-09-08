package logs

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/observability"
)

func TestTemplateStoreReportsUpsertOnCreateAndOnWiden(t *testing.T) {
	t.Run("should report an upsert on creation, with the literal text since nothing has generalized yet", func(t *testing.T) {
		m := observability.NewMetrics()
		store := NewTemplateStore(NewTree(testConfig(), m), m)

		_, first := store.Process("user 1 logged in")
		require.NotNil(t, first)
		require.Equal(t, "user 1 logged in", first.Text)
	})

	t.Run("should report a SECOND upsert when a later log widens the template", func(t *testing.T) {
		m := observability.NewMetrics()
		store := NewTemplateStore(NewTree(testConfig(), m), m)

		_, created := store.Process("user 1 logged in")
		require.NotNil(t, created)

		_, widened := store.Process("user 2 logged in")
		require.NotNil(t, widened, "a log line that widens the template must trigger a dictionary upsert -- "+
			"otherwise the persisted text goes stale the moment the template generalizes")
		require.Equal(t, created.TemplateID, widened.TemplateID)
		require.Equal(t, "user <*> logged in", widened.Text)
	})

	t.Run("should report NO upsert once the template is already fully general", func(t *testing.T) {
		m := observability.NewMetrics()
		store := NewTemplateStore(NewTree(testConfig(), m), m)

		store.Process("user 1 logged in")
		store.Process("user 2 logged in") // widens position 1 to <*>

		_, third := store.Process("user 3 logged in")
		require.Nil(t, third, "matching an already-general template must not report a redundant upsert")
	})
}

// TestTemplateStoreMeasuresStorageSaving is the phase's explicit ask:
// "measure and record the storage saving" from storing template_id+params
// instead of the rendered body.
func TestTemplateStoreMeasuresStorageSaving(t *testing.T) {
	t.Run("should show a large saving for a repetitive corpus", func(t *testing.T) {
		m := observability.NewMetrics()
		store := NewTemplateStore(NewTree(testConfig(), m), m)

		for i := 0; i < 10_000; i++ {
			store.Process(fmt.Sprintf(
				"user %d authenticated successfully from 10.0.0.%d via session token abc-%d",
				i, i%255, i))
		}

		raw := testutil.ToFloat64(m.LogBytesRaw)
		templated := testutil.ToFloat64(m.LogBytesTemplated)

		require.Positive(t, raw)
		require.Positive(t, templated)
		require.Less(t, templated, raw,
			"template_id+params must cost less than storing the rendered body for a repetitive corpus")

		ratio := raw / templated
		t.Logf("storage saving over %d repetitive log lines: raw=%.0f templated=%.0f ratio=%.2fx",
			10_000, raw, templated, ratio)
		require.Greaterf(t, ratio, 2.0, "expected a substantial saving on a highly repetitive corpus, got %.2fx", ratio)
	})
}

func TestTemplateStoreNewTemplateText(t *testing.T) {
	t.Run("should join the template tokens with spaces", func(t *testing.T) {
		m := observability.NewMetrics()
		store := NewTemplateStore(NewTree(testConfig(), m), m)

		_, nt := store.Process("health check ok")
		require.NotNil(t, nt)
		require.Equal(t, "health check ok", nt.Text)
		require.Equal(t, uint32(1), nt.TemplateID)
	})
}
