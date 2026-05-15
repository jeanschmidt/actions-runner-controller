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
	// queuedThresholdMinutes=0: include jobs queued for any duration
	// maxAgeDays=3: look at the last 3 days of data
	// orgs=["pytorch"]: scope to the pytorch GitHub org
	// repo="": all repos in the org
	defaultHUDAPIURL = "https://hud.pytorch.org/api/clickhouse/queued_jobs_aggregate" +
		"?parameters=%7B%22queuedThresholdMinutes%22%3A0%2C%22maxAgeDays%22%3A3%2C%22orgs%22%3A%5B%22pytorch%22%5D%2C%22repo%22%3A%22%22%7D"
	// hudResponseMaxBytes caps the JSON payload we will read from the
	// HUD API. A misbehaving or compromised endpoint must not be able
	// to OOM the listener by streaming an unbounded response.
	hudResponseMaxBytes = 10 * 1024 * 1024 // 10 MiB
)

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
	// baseURL is the configured URL with any `parameters=` query value
	// stripped out. We rebuild that query string per-request so each
	// scale set can pass its own runnerLabels filter.
	baseURL string
	// baseParams holds the parameters JSON decoded from the configured URL
	// (or an empty map if none was supplied). It is the seed that every
	// per-request parameter object is built from, so existing manifest
	// overrides (queuedThresholdMinutes, orgs, repo, maxAgeDays) continue
	// to apply.
	baseParams map[string]any
	token      string
	client     *http.Client
}

// NewHUDClient creates a new HUD API client with the given auth token.
// If hudURL carries a `parameters=<json>` query string, that JSON is parsed
// once and used as the per-request parameter seed; runnerLabels is then
// merged in at request time. URLs without a parameters query work too —
// the seed is then an empty object and the server applies its defaults.
func NewHUDClient(hudURL, token string) *HUDClient {
	base, params := splitHUDURL(hudURL)
	return &HUDClient{
		baseURL:    base,
		baseParams: params,
		token:      token,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// splitHUDURL pulls the `parameters=<json>` query value out of url and
// returns the bare URL plus the decoded parameter map. If parsing fails
// for any reason the input is returned untouched with an empty map —
// failure-open keeps a malformed manifest from breaking the listener
// entirely; the request will still go through with whatever the server
// considers default parameters.
func splitHUDURL(rawURL string) (string, map[string]any) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL, map[string]any{}
	}
	q := u.Query()
	paramsRaw := q.Get("parameters")
	q.Del("parameters")
	u.RawQuery = q.Encode()

	params := map[string]any{}
	if paramsRaw != "" {
		// HUD historically also accepts `parameters=[]` (empty array). Treat
		// any non-object payload as an empty seed map rather than erroring.
		_ = json.Unmarshal([]byte(paramsRaw), &params)
	}
	return u.String(), params
}

// buildURL serialises baseParams + the per-request runnerLabels into a
// fresh `?parameters=<json>` query on top of baseURL.
func (c *HUDClient) buildURL(labels []string) (string, error) {
	params := make(map[string]any, len(c.baseParams)+1)
	for k, v := range c.baseParams {
		params[k] = v
	}
	// Always set runnerLabels — non-nil even when empty so JSON encodes
	// `[]` not `null`. The server-side default of an empty array is "no
	// filter", which matches the previous full-aggregate behavior; once
	// the scale set has labels, those filter the response down to one
	// row, dropping the rest server-side.
	if labels == nil {
		labels = []string{}
	}
	params["runnerLabels"] = labels

	encoded, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("encoding HUD parameters: %w", err)
	}

	u, err := url.Parse(c.baseURL)
	if err != nil {
		return "", fmt.Errorf("parsing HUD base URL: %w", err)
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
// labels is also pushed into the request as the `runnerLabels` query
// parameter, so the server returns only matching rows. We still apply
// the local match below as a safety net (in case the server hasn't been
// upgraded yet, or rolls a query that ignores runnerLabels).
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
