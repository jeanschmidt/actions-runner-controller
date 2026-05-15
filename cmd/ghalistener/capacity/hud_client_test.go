package capacity

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHUDClient(t *testing.T, handler http.HandlerFunc) (*HUDClient, func()) {
	t.Helper()
	srv := httptest.NewServer(handler)
	client := NewHUDClient(srv.URL, "test-token")
	client.client.Timeout = 5 * time.Second
	return client, srv.Close
}

func TestGetQueuedJobsForLabels_HappyPath(t *testing.T) {
	rows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 10},
		{RunnerLabel: "linux.4xlarge", NumQueuedJobs: 5},
		{RunnerLabel: "linux.gpu.a100", NumQueuedJobs: 3},
	}
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "test-token", r.Header.Get("x-hud-internal-bot"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(rows)
	})
	defer cleanup()

	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{"linux.2xlarge"})
	require.NoError(t, err)
	assert.Equal(t, 10, total)
}

func TestGetQueuedJobsForLabels_NoMatchingLabels(t *testing.T) {
	rows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 10},
	}
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(rows)
	})
	defer cleanup()

	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{"windows.large"})
	require.NoError(t, err)
	assert.Equal(t, 0, total)
}

func TestGetQueuedJobsForLabels_MultipleMatchingLabels(t *testing.T) {
	rows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 10},
		{RunnerLabel: "linux.4xlarge", NumQueuedJobs: 5},
		{RunnerLabel: "linux.gpu.a100", NumQueuedJobs: 3},
	}
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(rows)
	})
	defer cleanup()

	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{"linux.2xlarge", "linux.gpu.a100"})
	require.NoError(t, err)
	assert.Equal(t, 13, total)
}

func TestGetQueuedJobsForLabels_EmptyLabels(t *testing.T) {
	client := NewHUDClient(defaultHUDAPIURL, "token")
	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{})
	require.NoError(t, err)
	assert.Equal(t, 0, total)
}

func TestGetQueuedJobsForLabels_ServerError(t *testing.T) {
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer cleanup()

	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{"linux.2xlarge"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
	assert.Equal(t, 0, total)
}

func TestGetQueuedJobsForLabels_Timeout(t *testing.T) {
	// Use a channel so the handler blocks until the test signals it to stop,
	// preventing httptest.Server.Close from waiting for the handler goroutine.
	done := make(chan struct{})
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		<-done
	})
	defer func() { close(done); cleanup() }()
	client.client.Timeout = 50 * time.Millisecond

	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{"linux.2xlarge"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HUD API request failed")
	assert.Equal(t, 0, total)
}

func TestGetQueuedJobsForLabels_MalformedJSON(t *testing.T) {
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("this is not json"))
	})
	defer cleanup()

	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{"linux.2xlarge"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decoding HUD response")
	assert.Equal(t, 0, total)
}

func TestGetQueuedJobsForLabels_EmptyToken(t *testing.T) {
	rows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 7},
	}
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		// Token header should be set even if empty.
		assert.Equal(t, "", r.Header.Get("x-hud-internal-bot"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(rows)
	})
	defer cleanup()
	client.token = ""

	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{"linux.2xlarge"})
	require.NoError(t, err)
	assert.Equal(t, 7, total)
}

// Each request must include `parameters=<json>` carrying the scale set's
// labels under `runnerLabels`, so the HUD server can filter rows server-side.
// Without this the listener was downloading the full aggregate (~10 MiB peak)
// and locally filtering — wasteful for 50 listeners all hitting at once.
func TestGetQueuedJobsForLabels_SendsRunnerLabelsInParameters(t *testing.T) {
	var got struct {
		RunnerLabels []string `json:"runnerLabels"`
	}
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		paramsRaw := r.URL.Query().Get("parameters")
		require.NotEmpty(t, paramsRaw, "request must carry a parameters query")
		require.NoError(t, json.Unmarshal([]byte(paramsRaw), &got))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode([]QueuedJobsForRunner{})
	})
	defer cleanup()

	_, err := client.GetQueuedJobsForLabels(
		context.Background(),
		[]string{"l-x86iavx512-32-256", "self-hosted"},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"l-x86iavx512-32-256", "self-hosted"}, got.RunnerLabels)
}

// Existing parameters baked into the configured HUDAPIURL must be carried
// forward verbatim. The OSDC manifest overrides queuedThresholdMinutes=0,
// maxAgeDays=3, orgs=["pytorch"], repo=""; losing any of those would change
// the autoscaler's behavior (e.g. ignoring jobs queued for <30 min by default).
func TestGetQueuedJobsForLabels_PreservesBaseParametersFromURL(t *testing.T) {
	baseURL := "" // populated once the test server is up
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paramsRaw := r.URL.Query().Get("parameters")
		require.NoError(t, json.Unmarshal([]byte(paramsRaw), &got))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode([]QueuedJobsForRunner{})
	}))
	defer srv.Close()
	baseURL = srv.URL +
		"/api?parameters=%7B%22queuedThresholdMinutes%22%3A0%2C%22maxAgeDays%22%3A3%2C%22orgs%22%3A%5B%22pytorch%22%5D%2C%22repo%22%3A%22%22%7D"

	client := NewHUDClient(baseURL, "tok")
	client.client.Timeout = 5 * time.Second
	_, err := client.GetQueuedJobsForLabels(context.Background(), []string{"some-label"})
	require.NoError(t, err)

	assert.EqualValues(t, 0, got["queuedThresholdMinutes"])
	assert.EqualValues(t, 3, got["maxAgeDays"])
	assert.Equal(t, []any{"pytorch"}, got["orgs"])
	assert.Equal(t, "", got["repo"])
	assert.Equal(t, []any{"some-label"}, got["runnerLabels"])
}

// A configured URL with no `parameters=` query is valid — the server
// applies its own defaults and the listener only contributes runnerLabels.
// This codepath is exercised by tests that pass `srv.URL` directly to
// NewHUDClient (no query string).
func TestGetQueuedJobsForLabels_NoBaseParameters_StillSendsRunnerLabels(t *testing.T) {
	var got map[string]any
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		paramsRaw := r.URL.Query().Get("parameters")
		require.NoError(t, json.Unmarshal([]byte(paramsRaw), &got))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode([]QueuedJobsForRunner{})
	})
	defer cleanup()

	_, err := client.GetQueuedJobsForLabels(context.Background(), []string{"only-label"})
	require.NoError(t, err)

	assert.Equal(t, []any{"only-label"}, got["runnerLabels"])
	// No other keys should appear when the URL didn't supply any.
	assert.Len(t, got, 1, "only runnerLabels should be set")
}

// The client must still filter locally, in case the HUD server is on an
// older revision that ignores the runnerLabels parameter and returns the
// full aggregate. Once test-infra rolls out the new query this becomes
// belt-and-suspenders, but during the rollout window it's load-bearing.
func TestGetQueuedJobsForLabels_LocalFilterAppliesWhenServerIgnoresRunnerLabels(t *testing.T) {
	rows := []QueuedJobsForRunner{
		{RunnerLabel: "wanted", NumQueuedJobs: 7},
		{RunnerLabel: "other", NumQueuedJobs: 42},
		{RunnerLabel: "wanted", NumQueuedJobs: 5},
	}
	client, cleanup := newTestHUDClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(rows)
	})
	defer cleanup()

	total, err := client.GetQueuedJobsForLabels(context.Background(), []string{"wanted"})
	require.NoError(t, err)
	assert.Equal(t, 12, total, "must sum only the wanted-label rows")
}

// Some legacy callers configured the URL with `parameters=[]` (empty JSON
// array) to mean "use server defaults". That decode path must not crash —
// we drop the value and only add runnerLabels.
func TestGetQueuedJobsForLabels_EmptyArrayBaseParameters_TreatedAsNoBase(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.Unmarshal([]byte(r.URL.Query().Get("parameters")), &got))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode([]QueuedJobsForRunner{})
	}))
	defer srv.Close()

	client := NewHUDClient(srv.URL+"/api?parameters=%5B%5D", "tok")
	client.client.Timeout = 5 * time.Second
	_, err := client.GetQueuedJobsForLabels(context.Background(), []string{"x"})
	require.NoError(t, err)
	assert.Equal(t, []any{"x"}, got["runnerLabels"])
}

// splitHUDURL is the seam between the configured URL and the per-request
// query rebuild. Direct unit tests keep regressions cheap to catch.
func TestSplitHUDURL(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantBase   string
		wantParams map[string]any
	}{
		{
			name:       "no_query",
			in:         "https://hud/api",
			wantBase:   "https://hud/api",
			wantParams: map[string]any{},
		},
		{
			name:       "parameters_object",
			in:         "https://hud/api?parameters=%7B%22maxAgeDays%22%3A3%7D",
			wantBase:   "https://hud/api",
			wantParams: map[string]any{"maxAgeDays": float64(3)},
		},
		{
			name:       "parameters_empty_array",
			in:         "https://hud/api?parameters=%5B%5D",
			wantBase:   "https://hud/api",
			wantParams: map[string]any{},
		},
		{
			name:       "other_query_preserved",
			in:         "https://hud/api?foo=bar&parameters=%7B%7D",
			wantBase:   "https://hud/api?foo=bar",
			wantParams: map[string]any{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base, params := splitHUDURL(tc.in)
			assert.Equal(t, tc.wantBase, base)
			assert.Equal(t, tc.wantParams, params)
		})
	}
}
