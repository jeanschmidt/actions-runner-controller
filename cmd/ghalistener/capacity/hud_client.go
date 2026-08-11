package capacity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultHUDAPIURL = "https://hud.pytorch.org/api/clickhouse/queued_jobs_aggregate"
	// Vercel Pro/Enterprise caps serverless function responses at 4.5 MB.
	// hud.pytorch.org runs on Vercel, so any real response is below this;
	// the cap is the listener's own guard against OOM if HUD moves off
	// Vercel or returns a runaway payload.
	hudResponseMaxBytes = 4_500_000
)

type hudRequestParams struct {
	QueuedThresholdMinutes int      `json:"queuedThresholdMinutes"`
	MaxAgeDays             int      `json:"maxAgeDays"`
	Orgs                   []string `json:"orgs"`
	Repo                   string   `json:"repo"`
	RunnerLabels           []string `json:"runnerLabels"`
}

func defaultHUDOrgs() []string { return []string{"pytorch"} }

func defaultHUDRequestParams() hudRequestParams {
	return hudRequestParams{
		QueuedThresholdMinutes: 0,
		MaxAgeDays:             1,
		Orgs:                   defaultHUDOrgs(),
		Repo:                   "",
	}
}

type QueuedJobsForRunner struct {
	RunnerLabel         string  `json:"runner_label"`
	Org                 string  `json:"org"`
	Repo                string  `json:"repo"`
	NumQueuedJobs       int     `json:"num_queued_jobs"`
	MinQueueTimeMinutes float64 `json:"min_queue_time_minutes"`
	MaxQueueTimeMinutes float64 `json:"max_queue_time_minutes"`
}

type HUDClient struct {
	url    string
	token  string
	orgs   []string
	client *http.Client
}

func NewHUDClient(hudURL, token string, orgs ...string) *HUDClient {
	return &HUDClient{
		url:    hudURL,
		token:  token,
		orgs:   orgs,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *HUDClient) buildURL(labels []string, thresholdMinutes int) (string, error) {
	p := defaultHUDRequestParams()
	// Drop empty/whitespace orgs so a stray "" can never reach the wire:
	// HUD treats orgs:[""] as "no filter → every org", which would size a
	// listener's capacity from every org's queue. Empty after filtering
	// leaves the in-code default (defaultHUDOrgs) untouched.
	filtered := make([]string, 0, len(c.orgs))
	for _, org := range c.orgs {
		if trimmed := strings.TrimSpace(org); trimmed != "" {
			filtered = append(filtered, trimmed)
		}
	}
	if len(filtered) > 0 {
		p.Orgs = filtered
	}
	p.QueuedThresholdMinutes = thresholdMinutes
	if labels == nil {
		// Marshal as `[]`, not `null`.
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

// GetQueuedJobsForLabels pushes labels into the request's runnerLabels so
// the server filters server-side. The local re-filter below is defense in
// depth against a future server-side regression that ignores the filter.
func (c *HUDClient) GetQueuedJobsForLabels(ctx context.Context, labels []string) (int, error) {
	return c.GetQueuedJobsForLabelsWithThreshold(ctx, labels, 0)
}

// GetQueuedJobsForLabelsWithThreshold sends thresholdMinutes to HUD as
// queuedThresholdMinutes; only jobs queued for at least that long are
// counted. thresholdMinutes=0 means count every queued job.
func (c *HUDClient) GetQueuedJobsForLabelsWithThreshold(ctx context.Context, labels []string, thresholdMinutes int) (int, error) {
	if len(labels) == 0 {
		return 0, nil
	}

	reqURL, err := c.buildURL(labels, thresholdMinutes)
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
	if err := json.NewDecoder(io.LimitReader(resp.Body, hudResponseMaxBytes)).Decode(&rows); err != nil {
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
