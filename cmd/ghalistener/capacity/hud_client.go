package capacity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	// defaultHUDAPIURL is the bare endpoint with no query string. Per-request
	// parameters (including runnerLabels) are JSON-encoded and attached at
	// call time — see hudRequestParams.
	defaultHUDAPIURL = "https://hud.pytorch.org/api/clickhouse/queued_jobs_aggregate"
	// hudResponseMaxBytes caps the JSON payload we will read from the HUD
	// API. A misbehaving or compromised endpoint must not be able to OOM
	// the listener by streaming an unbounded response.
	hudResponseMaxBytes = 10 * 1024 * 1024 // 10 MiB
)

// hudRequestParams is the JSON object sent as the `parameters` query
// argument to the HUD API. The non-RunnerLabels fields match the values
// the OSDC autoscaler needs (any-duration queued jobs, pytorch org only,
// 3-day window). Change them here in code if behavior needs to differ —
// keeping them in Go (not in a baked-in URL) is what lets us drop the
// previous URL-parameter-parsing dance.
type hudRequestParams struct {
	QueuedThresholdMinutes int      `json:"queuedThresholdMinutes"`
	MaxAgeDays             int      `json:"maxAgeDays"`
	Orgs                   []string `json:"orgs"`
	Repo                   string   `json:"repo"`
	RunnerLabels           []string `json:"runnerLabels"`
}

// defaultHUDRequestParams returns the per-request parameter struct seeded
// with the OSDC defaults. The caller fills in RunnerLabels for the scale
// set before serialisation.
func defaultHUDRequestParams() hudRequestParams {
	return hudRequestParams{
		QueuedThresholdMinutes: 0,
		MaxAgeDays:             3,
		Orgs:                   []string{"pytorch"},
		Repo:                   "",
	}
}

// QueuedJobsForRunner represents a single row from the HUD API response.
type QueuedJobsForRunner struct {
	RunnerLabel         string  `json:"runner_label"`
	Org                 string  `json:"org"`
	Repo                string  `json:"repo"`
	NumQueuedJobs       int     `json:"num_queued_jobs"`
	MinQueueTimeMinutes float64 `json:"min_queue_time_minutes"`
	MaxQueueTimeMinutes float64 `json:"max_queue_time_minutes"`
}

// HUDClient is an HTTP client for the PyTorch HUD API that returns
// aggregate queued job counts per runner label.
type HUDClient struct {
	url    string
	token  string
	client *http.Client
}

// NewHUDClient creates a new HUD API client. hudURL is the bare endpoint
// (no `parameters=` query); parameters are assembled per-request from the
// hudRequestParams struct above. Any query string already present on the
// supplied URL is preserved verbatim, except that `parameters` is always
// overwritten.
func NewHUDClient(hudURL, token string) *HUDClient {
	return &HUDClient{
		url:    hudURL,
		token:  token,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// buildURL serialises the default parameters with RunnerLabels=labels into
// the `parameters` query argument on top of c.url.
func (c *HUDClient) buildURL(labels []string) (string, error) {
	p := defaultHUDRequestParams()
	// Always non-nil so JSON encodes as `[]` not `null`.
	if labels == nil {
		labels = []string{}
	}
	p.RunnerLabels = labels

	encoded, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encoding HUD parameters: %w", err)
	}

	u, err := url.Parse(c.url)
	if err != nil {
		return "", fmt.Errorf("parsing HUD URL: %w", err)
	}
	q := u.Query()
	q.Set("parameters", string(encoded))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// GetQueuedJobsForLabels queries the HUD API and returns the total
// number of queued jobs matching any of the provided runner labels.
// On any error the caller receives (0, err) and decides the fallback.
//
// labels is also pushed into the request as the `runnerLabels` field of
// the `parameters` JSON so the server returns only matching rows. We
// still apply the local match below as a safety net (in case the server
// hasn't been upgraded yet, or rolls a query that ignores runnerLabels).
func (c *HUDClient) GetQueuedJobsForLabels(ctx context.Context, labels []string) (int, error) {
	if len(labels) == 0 {
		return 0, nil
	}

	reqURL, err := c.buildURL(labels)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, fmt.Errorf("building HUD request: %w", err)
	}
	req.Header.Set("x-hud-internal-bot", c.token)

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("HUD API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HUD API returned status %d", resp.StatusCode)
	}

	var rows []QueuedJobsForRunner
	body := io.LimitReader(resp.Body, hudResponseMaxBytes)
	if err := json.NewDecoder(body).Decode(&rows); err != nil {
		return 0, fmt.Errorf("decoding HUD response: %w", err)
	}

	labelSet := make(map[string]struct{}, len(labels))
	for _, l := range labels {
		labelSet[l] = struct{}{}
	}

	total := 0
	for _, row := range rows {
		if _, ok := labelSet[row.RunnerLabel]; ok {
			total += row.NumQueuedJobs
		}
	}
	return total, nil
}
