package capacity

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
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

// Each request must carry every parameter the server expects — both the
// scale set's runnerLabels and the OSDC autoscaler defaults. Asserting on
// the wire payload keeps a future edit to defaultHUDRequestParams() from
// silently changing the autoscaler's behavior.
func TestGetQueuedJobsForLabels_SendsExpectedParameters(t *testing.T) {
	var got struct {
		QueuedThresholdMinutes int      `json:"queuedThresholdMinutes"`
		MaxAgeDays             int      `json:"maxAgeDays"`
		Orgs                   []string `json:"orgs"`
		Repo                   string   `json:"repo"`
		RunnerLabels           []string `json:"runnerLabels"`
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

	assert.Equal(t, 0, got.QueuedThresholdMinutes, "any-duration queued jobs (OSDC override of server default=30)")
	assert.Equal(t, 3, got.MaxAgeDays)
	assert.Equal(t, []string{"pytorch"}, got.Orgs)
	assert.Equal(t, "", got.Repo)
	assert.Equal(t, []string{"l-x86iavx512-32-256", "self-hosted"}, got.RunnerLabels)
}

// The client must still filter locally, in case the HUD server is on an
// older revision that ignores the runnerLabels parameter and returns the
// full aggregate. Once test-infra rolls out the new query this is
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

// Tolerate manifests that still carry the legacy `?parameters=<json>` baked
// into the URL: buildURL overwrites the `parameters` query value, so the
// old payload is dropped without breaking anything. This means rolling
// this code out before the chart-side URL cleanup is safe.
func TestBuildURL_OverwritesPreexistingParametersQuery(t *testing.T) {
	legacyURL := "https://hud.example/api/queued_jobs_aggregate" +
		"?parameters=%7B%22maxAgeDays%22%3A99%2C%22orgs%22%3A%5B%22stale%22%5D%7D"
	client := NewHUDClient(legacyURL, "tok")

	built, err := client.buildURL([]string{"label-1"})
	require.NoError(t, err)

	u, err := neturl.Parse(built)
	require.NoError(t, err)
	paramsRaw := u.Query().Get("parameters")
	require.NotEmpty(t, paramsRaw)

	var got hudRequestParams
	require.NoError(t, json.Unmarshal([]byte(paramsRaw), &got))

	// Stale values from the legacy URL must NOT leak through; we always
	// emit the in-code defaults plus the per-request runnerLabels.
	assert.Equal(t, 3, got.MaxAgeDays, "stale URL-embedded maxAgeDays must be ignored")
	assert.Equal(t, []string{"pytorch"}, got.Orgs, "stale URL-embedded orgs must be ignored")
	assert.Equal(t, []string{"label-1"}, got.RunnerLabels)
}

// Empty runnerLabels must serialise as `[]`, not `null`. The HUD query
// treats a null parameter as missing and applies its server-side default
// (which for runnerLabels=[] is "no filter") — but emitting null instead
// of [] is inconsistent and risks the server rejecting the request.
func TestBuildURL_EmptyRunnerLabelsSerialisesAsEmptyArray(t *testing.T) {
	client := NewHUDClient("https://hud.example/api", "tok")

	for _, labels := range [][]string{nil, {}} {
		built, err := client.buildURL(labels)
		require.NoError(t, err)
		u, err := neturl.Parse(built)
		require.NoError(t, err)
		paramsRaw := u.Query().Get("parameters")
		// Look at the raw JSON, not just the decoded struct: only the wire
		// form can distinguish `[]` from `null`.
		assert.Contains(t, paramsRaw, `"runnerLabels":[]`,
			"runnerLabels must serialise as [] (got %q)", paramsRaw)
	}
}

// defaultHUDRequestParams is the single source of truth for non-label
// parameters. A direct test pins the values so a future edit shows up
// here, not as a surprise in production autoscaler behavior.
func TestDefaultHUDRequestParams(t *testing.T) {
	p := defaultHUDRequestParams()
	assert.Equal(t, 0, p.QueuedThresholdMinutes,
		"OSDC needs every queued job, not just >=30min (server default)")
	assert.Equal(t, 3, p.MaxAgeDays)
	assert.Equal(t, []string{"pytorch"}, p.Orgs)
	assert.Equal(t, "", p.Repo)
	assert.Nil(t, p.RunnerLabels, "RunnerLabels is filled in per-request")
}
