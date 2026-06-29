package capacity

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// proactiveCapacityHardCap is the absolute maximum allowed value;
	// anything larger is clamped to prevent runaway placeholder creation.
	proactiveCapacityHardCap = 1000
	// proactiveCapacityWarnThreshold triggers a warning log but does
	// not clamp — operators may legitimately need >100 in surge cases.
	proactiveCapacityWarnThreshold = 100
	// defaultHUDFailureMultiplier is applied to ProactiveCapacity when the
	// HUD API is unreachable. A value >1 keeps placeholder capacity above
	// the proactive baseline during a HUD outage; outer caps (MaxRunners
	// headroom, MaxBurstCapacity) bound the absolute blast radius.
	defaultHUDFailureMultiplier = 3
)

// Config holds all configuration for the capacity monitor.
// Fields marked "set by main.go" are populated after ConfigFromEnv returns.
type Config struct {
	Enabled             bool
	ProactiveCapacity   int
	RecalculateInterval time.Duration
	ReportInterval      time.Duration
	PlaceholderTimeout  time.Duration
	MaxRunners          int

	// MaxBurstCapacity caps the maximum number of placeholder pairs (running + pending)
	// the provisioner will create per reconcile cycle. 0 means no cap.
	// Used to prevent burst node provisioning from overloading downstream services
	// (git-cache rsync connection pool, Harbor manifest fetches, pypi-cache).
	MaxBurstCapacity int

	// Workflow pod resources (for placeholder-workflow sizing)
	WorkflowCPU    string
	WorkflowMemory string
	WorkflowGPU    int
	WorkflowDisk   string

	// Workflow scheduling ensures placeholder availability is representative.
	WorkflowSchedulerName string

	// Runner pod resources (for placeholder-runner sizing)
	RunnerCPU    string
	RunnerMemory string

	// Node placement.
	// NodeFleet is the workflow-pool fleet, used for placeholder-workflow pods.
	// Per-scale-set value (e.g. g4dn, c7a, m8g).
	// RunnerNodeFleet is the runner-pool fleet, used for placeholder-runner pods.
	// Cluster-wide value (currently c7i-runner) — same across all scale sets.
	NodeFleet       string
	RunnerNodeFleet string
	RunnerClass     string

	// Scale set info (set by main.go, not env vars)
	ScaleSetID     int
	ScaleSetLabels []string
	Namespace      string // runner namespace (EphemeralRunnerSetNamespace)

	// Scale set name for label selectors (set by main.go)
	ScaleSetName string

	// HUD API
	HUDAPIURL            string
	HUDAPIToken          string
	HUDFailureMultiplier int

	// HUDFailureBaseCapacity is an additive baseline applied to the fallback
	// formula when the HUD API is unreachable. Lets operators provision a
	// flat surge floor even when ProactiveCapacity is 0 (where the
	// multiplier alone would yield 0). Clamped to [0, proactiveCapacityHardCap].
	HUDFailureBaseCapacity int

	// ClusterIndex is this cluster's position in [0, ClusterCount). Used to
	// deterministically slice the fresh portion of the HUD-reported queue
	// across peer clusters that all serve the same runner labels.
	ClusterIndex int

	// ClusterCount is the number of peer clusters serving the same labels.
	// When ClusterCount > 1 and AgeThresholdSeconds > 0, each cluster claims
	// only its 1/ClusterCount slice of fresh jobs (plus 100% of aged jobs).
	ClusterCount int

	// AgeThresholdSeconds is the queue-age boundary between "fresh" jobs
	// (sharded across clusters) and "aged" jobs (claimed in full by every
	// cluster). 0 disables sharding entirely.
	AgeThresholdSeconds int
}

// ConfigFromEnv reads capacity monitor configuration from environment
// variables. Fields that come from the listener config (MaxRunners,
// ScaleSetID, ScaleSetLabels, Namespace, ScaleSetName) are left at
// zero values and must be set by the caller.
func ConfigFromEnv() Config {
	c := Config{
		Enabled:                envBool("CAPACITY_AWARE_ENABLED", false),
		ProactiveCapacity:      envInt("CAPACITY_AWARE_PROACTIVE_CAPACITY", 0),
		MaxBurstCapacity:       envInt("CAPACITY_AWARE_MAX_BURST_CAPACITY", 0),
		RecalculateInterval:    envDuration("CAPACITY_AWARE_RECALCULATE_INTERVAL", 60*time.Second),
		ReportInterval:         envDuration("CAPACITY_AWARE_REPORT_INTERVAL", 5*time.Second),
		PlaceholderTimeout:     envDuration("CAPACITY_AWARE_PLACEHOLDER_TIMEOUT", 5*time.Minute),
		WorkflowCPU:            envString("CAPACITY_AWARE_WORKFLOW_CPU", ""),
		WorkflowMemory:         envString("CAPACITY_AWARE_WORKFLOW_MEMORY", ""),
		WorkflowGPU:            envInt("CAPACITY_AWARE_WORKFLOW_GPU", 0),
		WorkflowDisk:           envString("CAPACITY_AWARE_WORKFLOW_DISK", ""),
		WorkflowSchedulerName:  envString("CAPACITY_AWARE_WORKFLOW_SCHEDULER_NAME", ""),
		RunnerCPU:              envString("CAPACITY_AWARE_RUNNER_CPU", "750m"),
		RunnerMemory:           envString("CAPACITY_AWARE_RUNNER_MEMORY", "512Mi"),
		NodeFleet:              envString("CAPACITY_AWARE_NODE_FLEET", ""),
		RunnerNodeFleet:        envString("CAPACITY_AWARE_RUNNER_NODE_FLEET", ""),
		RunnerClass:            envString("CAPACITY_AWARE_RUNNER_CLASS", ""),
		HUDAPIURL:              envString("CAPACITY_AWARE_HUD_API_URL", defaultHUDAPIURL),
		HUDAPIToken:            envString("CAPACITY_AWARE_HUD_API_TOKEN", ""),
		HUDFailureMultiplier:   envInt("CAPACITY_AWARE_HUD_FAILURE_MULTIPLIER", defaultHUDFailureMultiplier),
		HUDFailureBaseCapacity: envInt("CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY", 0),
		ClusterIndex:           envInt("CAPACITY_AWARE_CLUSTER_INDEX", 0),
		ClusterCount:           envInt("CAPACITY_AWARE_CLUSTER_COUNT", 1),
		AgeThresholdSeconds:    envInt("CAPACITY_AWARE_AGE_THRESHOLD_SECONDS", 0),
	}

	if c.ProactiveCapacity < 0 {
		slog.Warn("CAPACITY_AWARE_PROACTIVE_CAPACITY is negative, clamping to 0",
			"original", c.ProactiveCapacity)
		c.ProactiveCapacity = 0
	}
	if c.ProactiveCapacity > proactiveCapacityHardCap {
		slog.Warn("CAPACITY_AWARE_PROACTIVE_CAPACITY exceeds hard cap, clamping",
			"original", c.ProactiveCapacity, "cap", proactiveCapacityHardCap)
		c.ProactiveCapacity = proactiveCapacityHardCap
	} else if c.ProactiveCapacity > proactiveCapacityWarnThreshold {
		slog.Warn("CAPACITY_AWARE_PROACTIVE_CAPACITY is unusually high",
			"value", c.ProactiveCapacity, "warnThreshold", proactiveCapacityWarnThreshold)
	}

	if c.HUDFailureMultiplier < 1 {
		slog.Warn("CAPACITY_AWARE_HUD_FAILURE_MULTIPLIER must be >= 1, clamping",
			"original", c.HUDFailureMultiplier, "clampedTo", 1)
		c.HUDFailureMultiplier = 1
	}

	if c.HUDFailureBaseCapacity < 0 {
		slog.Warn("CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY is negative, clamping to 0",
			"original", c.HUDFailureBaseCapacity)
		c.HUDFailureBaseCapacity = 0
	}
	if c.HUDFailureBaseCapacity > proactiveCapacityHardCap {
		slog.Warn("CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY exceeds hard cap, clamping",
			"original", c.HUDFailureBaseCapacity, "cap", proactiveCapacityHardCap)
		c.HUDFailureBaseCapacity = proactiveCapacityHardCap
	} else if c.HUDFailureBaseCapacity > proactiveCapacityWarnThreshold {
		slog.Warn("CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY is unusually high",
			"value", c.HUDFailureBaseCapacity, "warnThreshold", proactiveCapacityWarnThreshold)
	}

	clampShardingFields(&c)

	return c
}

// Validate sanitizes fields populated by the caller (after ConfigFromEnv
// returns) and enforces required env vars when capacity-aware mode is
// enabled. Returns an error for any unrecoverable configuration problem.
//
// Side-effect: clamps negative MaxRunners to 0.
func (c *Config) Validate() error {
	if c.MaxRunners < 0 {
		slog.Warn("MaxRunners is negative, clamping to 0",
			"original", c.MaxRunners)
		c.MaxRunners = 0
	}
	if c.MaxBurstCapacity < 0 {
		slog.Warn("MaxBurstCapacity is negative, clamping to 0", "original", c.MaxBurstCapacity)
		c.MaxBurstCapacity = 0
	}
	if c.HUDFailureMultiplier < 1 {
		slog.Warn("HUDFailureMultiplier must be >= 1, clamping",
			"original", c.HUDFailureMultiplier, "clampedTo", 1)
		c.HUDFailureMultiplier = 1
	}
	if c.HUDFailureBaseCapacity < 0 {
		slog.Warn("HUDFailureBaseCapacity is negative, clamping to 0",
			"original", c.HUDFailureBaseCapacity)
		c.HUDFailureBaseCapacity = 0
	}

	clampShardingFields(c)

	if c.Enabled && c.RunnerNodeFleet == "" {
		// Hard requirement: the runner-pool fleet drives placeholder-runner
		// pod placement. Falling back to NodeFleet (the workflow-pool) would
		// silently land runner placeholders on the wrong pool — defeating the
		// topology separation that this config is here to provide.
		return errors.New(
			"CAPACITY_AWARE_RUNNER_NODE_FLEET is required when CAPACITY_AWARE_ENABLED=true",
		)
	}
	return nil
}

func envString(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return b
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	i, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return i
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return d
}

// clampShardingFields normalises the three cluster-aware sharding knobs so
// downstream arithmetic (ClusterIndex / ClusterCount, modulo, slice math)
// never encounters values that would underflow or panic. Callers that pass
// in nonsensical values get warn-and-clamp behaviour rather than a runtime
// error.
func clampShardingFields(c *Config) {
	if c.ClusterCount < 1 {
		slog.Warn("ClusterCount must be >= 1, clamping",
			"original", c.ClusterCount, "clampedTo", 1)
		c.ClusterCount = 1
	}
	if c.ClusterIndex < 0 {
		slog.Warn("ClusterIndex is negative, clamping to 0",
			"original", c.ClusterIndex)
		c.ClusterIndex = 0
	}
	if c.ClusterIndex >= c.ClusterCount {
		slog.Warn("ClusterIndex must be < ClusterCount, clamping",
			"originalIndex", c.ClusterIndex,
			"clusterCount", c.ClusterCount,
			"clampedTo", c.ClusterCount-1)
		c.ClusterIndex = c.ClusterCount - 1
	}
	if c.AgeThresholdSeconds < 0 {
		slog.Warn("AgeThresholdSeconds is negative, clamping to 0",
			"original", c.AgeThresholdSeconds)
		c.AgeThresholdSeconds = 0
	}
	if c.AgeThresholdSeconds > 0 && c.AgeThresholdSeconds < 60 {
		slog.Warn("AgeThresholdSeconds below 60 rounds down to 0 minutes in the HUD API (which uses minute granularity) and would make every cluster claim 100% of the queue as aged, disabling sharding; clamping to 60",
			"original", c.AgeThresholdSeconds, "clampedTo", 60)
		c.AgeThresholdSeconds = 60
	}
}
