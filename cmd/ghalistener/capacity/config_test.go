package capacity

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setEnvs(t *testing.T, envs map[string]string) {
	t.Helper()
	for k, v := range envs {
		prev, existed := os.LookupEnv(k)
		os.Setenv(k, v)
		if existed {
			t.Cleanup(func() { os.Setenv(k, prev) })
		} else {
			t.Cleanup(func() { os.Unsetenv(k) })
		}
	}
}

func unsetEnvs(t *testing.T, keys []string) {
	t.Helper()
	for _, k := range keys {
		prev, existed := os.LookupEnv(k)
		os.Unsetenv(k)
		if existed {
			t.Cleanup(func() { os.Setenv(k, prev) })
		}
	}
}

func TestConfigFromEnv_Defaults(t *testing.T) {
	keys := []string{
		"CAPACITY_AWARE_ENABLED",
		"CAPACITY_AWARE_PROACTIVE_CAPACITY",
		"CAPACITY_AWARE_RECALCULATE_INTERVAL",
		"CAPACITY_AWARE_PLACEHOLDER_TIMEOUT",
		"CAPACITY_AWARE_WORKFLOW_CPU",
		"CAPACITY_AWARE_WORKFLOW_MEMORY",
		"CAPACITY_AWARE_WORKFLOW_GPU",
		"CAPACITY_AWARE_WORKFLOW_DISK",
		"CAPACITY_AWARE_WORKFLOW_SCHEDULER_NAME",
		"CAPACITY_AWARE_RUNNER_CPU",
		"CAPACITY_AWARE_RUNNER_MEMORY",
		"CAPACITY_AWARE_NODE_FLEET",
		"CAPACITY_AWARE_RUNNER_NODE_FLEET",
		"CAPACITY_AWARE_RUNNER_CLASS",
		"CAPACITY_AWARE_HUD_API_TOKEN",
		"CAPACITY_AWARE_HUD_FAILURE_MULTIPLIER",
		"CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY",
	}
	unsetEnvs(t, keys)

	cfg := ConfigFromEnv()

	assert.False(t, cfg.Enabled, "Enabled default")
	assert.Equal(t, 0, cfg.ProactiveCapacity, "ProactiveCapacity default")
	assert.Equal(t, 60*time.Second, cfg.RecalculateInterval, "RecalculateInterval default")
	assert.Equal(t, 5*time.Minute, cfg.PlaceholderTimeout, "PlaceholderTimeout default")
	assert.Equal(t, "", cfg.WorkflowCPU, "WorkflowCPU default")
	assert.Equal(t, "", cfg.WorkflowMemory, "WorkflowMemory default")
	assert.Equal(t, 0, cfg.WorkflowGPU, "WorkflowGPU default")
	assert.Equal(t, "", cfg.WorkflowDisk, "WorkflowDisk default")
	assert.Equal(t, "", cfg.WorkflowSchedulerName, "WorkflowSchedulerName default")
	assert.Equal(t, "750m", cfg.RunnerCPU, "RunnerCPU default")
	assert.Equal(t, "512Mi", cfg.RunnerMemory, "RunnerMemory default")
	assert.Equal(t, "", cfg.NodeFleet, "NodeFleet default")
	assert.Equal(t, "", cfg.RunnerNodeFleet, "RunnerNodeFleet default")
	assert.Equal(t, "", cfg.RunnerClass, "RunnerClass default")
	assert.Equal(t, "", cfg.HUDAPIToken, "HUDAPIToken default")
	assert.Equal(t, defaultHUDFailureMultiplier, cfg.HUDFailureMultiplier, "HUDFailureMultiplier default")
	assert.Equal(t, 0, cfg.HUDFailureBaseCapacity, "HUDFailureBaseCapacity default")
	// Fields set by main.go should be zero values.
	assert.Equal(t, 0, cfg.MaxRunners, "MaxRunners zero")
	assert.Equal(t, 0, cfg.ScaleSetID, "ScaleSetID zero")
	assert.Nil(t, cfg.ScaleSetLabels, "ScaleSetLabels nil")
	assert.Equal(t, "", cfg.Namespace, "Namespace zero")
	assert.Equal(t, "", cfg.ScaleSetName, "ScaleSetName zero")
}

func TestConfigFromEnv_AllSet(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_ENABLED":                   "true",
		"CAPACITY_AWARE_PROACTIVE_CAPACITY":        "5",
		"CAPACITY_AWARE_RECALCULATE_INTERVAL":      "10s",
		"CAPACITY_AWARE_PLACEHOLDER_TIMEOUT":       "2m",
		"CAPACITY_AWARE_WORKFLOW_CPU":              "4",
		"CAPACITY_AWARE_WORKFLOW_MEMORY":           "8Gi",
		"CAPACITY_AWARE_WORKFLOW_GPU":              "2",
		"CAPACITY_AWARE_WORKFLOW_DISK":             "100Gi",
		"CAPACITY_AWARE_WORKFLOW_SCHEDULER_NAME":   "numa-scheduler",
		"CAPACITY_AWARE_RUNNER_CPU":                "1",
		"CAPACITY_AWARE_RUNNER_MEMORY":             "1Gi",
		"CAPACITY_AWARE_NODE_FLEET":                "gpu-fleet",
		"CAPACITY_AWARE_RUNNER_NODE_FLEET":         "c7i-runner",
		"CAPACITY_AWARE_RUNNER_CLASS":              "gpu-large",
		"CAPACITY_AWARE_HUD_API_TOKEN":             "secret-token",
		"CAPACITY_AWARE_HUD_FAILURE_MULTIPLIER":    "5",
		"CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY": "15",
	})

	cfg := ConfigFromEnv()

	assert.True(t, cfg.Enabled)
	assert.Equal(t, 5, cfg.ProactiveCapacity)
	assert.Equal(t, 10*time.Second, cfg.RecalculateInterval)
	assert.Equal(t, 2*time.Minute, cfg.PlaceholderTimeout)
	assert.Equal(t, "4", cfg.WorkflowCPU)
	assert.Equal(t, "8Gi", cfg.WorkflowMemory)
	assert.Equal(t, 2, cfg.WorkflowGPU)
	assert.Equal(t, "100Gi", cfg.WorkflowDisk)
	assert.Equal(t, "numa-scheduler", cfg.WorkflowSchedulerName)
	assert.Equal(t, "1", cfg.RunnerCPU)
	assert.Equal(t, "1Gi", cfg.RunnerMemory)
	assert.Equal(t, "gpu-fleet", cfg.NodeFleet)
	assert.Equal(t, "c7i-runner", cfg.RunnerNodeFleet)
	assert.Equal(t, "gpu-large", cfg.RunnerClass)
	assert.Equal(t, "secret-token", cfg.HUDAPIToken)
	assert.Equal(t, 5, cfg.HUDFailureMultiplier)
	assert.Equal(t, 15, cfg.HUDFailureBaseCapacity)
}

func TestConfigFromEnv_InvalidValues_FallbackToDefaults(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_ENABLED":                   "not-a-bool",
		"CAPACITY_AWARE_PROACTIVE_CAPACITY":        "not-an-int",
		"CAPACITY_AWARE_RECALCULATE_INTERVAL":      "not-a-duration",
		"CAPACITY_AWARE_PLACEHOLDER_TIMEOUT":       "999",
		"CAPACITY_AWARE_WORKFLOW_GPU":              "abc",
		"CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY": "not-a-number",
	})

	cfg := ConfigFromEnv()

	assert.False(t, cfg.Enabled, "invalid bool falls back to false")
	assert.Equal(t, 0, cfg.ProactiveCapacity, "invalid int falls back to 0")
	assert.Equal(t, 60*time.Second, cfg.RecalculateInterval, "invalid duration falls back to 60s")
	assert.Equal(t, 5*time.Minute, cfg.PlaceholderTimeout, "invalid duration falls back to 5m")
	assert.Equal(t, 0, cfg.WorkflowGPU, "invalid int falls back to 0")
	assert.Equal(t, 0, cfg.HUDFailureBaseCapacity, "invalid int falls back to 0")
}

func TestConfigFromEnv_WhitespaceTrimmmed(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_ENABLED":                   "  true  ",
		"CAPACITY_AWARE_PROACTIVE_CAPACITY":        "  3  ",
		"CAPACITY_AWARE_NODE_FLEET":                "  my-fleet  ",
		"CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY": "  7  ",
	})

	cfg := ConfigFromEnv()

	assert.True(t, cfg.Enabled)
	assert.Equal(t, 3, cfg.ProactiveCapacity)
	assert.Equal(t, "my-fleet", cfg.NodeFleet)
	assert.Equal(t, 7, cfg.HUDFailureBaseCapacity)
}

// Negative ProactiveCapacity must be clamped to 0 — never used as a
// negative (which would underflow downstream arithmetic).
func TestConfigFromEnv_ProactiveCapacity_NegativeClampedToZero(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_PROACTIVE_CAPACITY": "-5",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 0, cfg.ProactiveCapacity,
		"negative ProactiveCapacity must clamp to 0")
}

// Negative HUDFailureBaseCapacity must be clamped to 0 — same guard
// rationale as ProactiveCapacity (no negative values downstream).
func TestConfigFromEnv_HUDFailureBaseCapacity_NegativeClampedToZero(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY": "-5",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 0, cfg.HUDFailureBaseCapacity,
		"negative HUDFailureBaseCapacity must clamp to 0")
}

// HUDFailureMultiplier must be >= 1 — a value below 1 would never produce
// over-provisioning on HUD failure, defeating the purpose of the fallback.
func TestConfigFromEnv_HUDFailureMultiplier_BelowOneClampedToOne(t *testing.T) {
	t.Run("negative", func(t *testing.T) {
		setEnvs(t, map[string]string{
			"CAPACITY_AWARE_HUD_FAILURE_MULTIPLIER": "-5",
		})

		cfg := ConfigFromEnv()

		assert.Equal(t, 1, cfg.HUDFailureMultiplier,
			"negative HUDFailureMultiplier must clamp to 1")
	})
	t.Run("zero", func(t *testing.T) {
		setEnvs(t, map[string]string{
			"CAPACITY_AWARE_HUD_FAILURE_MULTIPLIER": "0",
		})

		cfg := ConfigFromEnv()

		assert.Equal(t, 1, cfg.HUDFailureMultiplier,
			"zero HUDFailureMultiplier must clamp to 1")
	})
}

// Values above the hard cap (1000) must be clamped — protects against
// runaway placeholder creation from a misconfiguration.
func TestConfigFromEnv_ProactiveCapacity_AboveHardCapClamped(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_PROACTIVE_CAPACITY": "5000",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, proactiveCapacityHardCap, cfg.ProactiveCapacity,
		"value above hard cap must clamp to %d", proactiveCapacityHardCap)
}

// Values exactly at the hard cap are allowed (boundary).
func TestConfigFromEnv_ProactiveCapacity_AtHardCapAllowed(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_PROACTIVE_CAPACITY": "1000",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, proactiveCapacityHardCap, cfg.ProactiveCapacity,
		"value exactly at hard cap must be preserved")
}

// Values above the warn threshold (100) but below the hard cap (1000)
// are allowed unchanged — operators may legitimately need this in surge.
func TestConfigFromEnv_ProactiveCapacity_AboveWarnAllowed(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_PROACTIVE_CAPACITY": "250",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 250, cfg.ProactiveCapacity,
		"values between warn threshold and hard cap are allowed")
}

// Values above the hard cap (1000) must be clamped — protects against
// runaway placeholder creation from a misconfiguration.
func TestConfigFromEnv_HUDFailureBaseCapacity_AboveHardCapClamped(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY": "5000",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, proactiveCapacityHardCap, cfg.HUDFailureBaseCapacity,
		"value above hard cap must clamp to %d", proactiveCapacityHardCap)
}

// Values exactly at the hard cap are allowed (boundary).
func TestConfigFromEnv_HUDFailureBaseCapacity_AtHardCapAllowed(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY": "1000",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, proactiveCapacityHardCap, cfg.HUDFailureBaseCapacity,
		"value exactly at hard cap must be preserved")
}

// Values above the warn threshold (100) but below the hard cap (1000)
// are allowed unchanged — operators may legitimately need this in surge.
func TestConfigFromEnv_HUDFailureBaseCapacity_AboveWarnAllowed(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY": "250",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 250, cfg.HUDFailureBaseCapacity,
		"values between warn threshold and hard cap are allowed")
}

// Validate() must enforce HUDFailureMultiplier >= 1 for callers that
// construct Config programmatically (bypassing ConfigFromEnv's clamp).
func TestConfig_Validate_HUDFailureMultiplierClampedBelowOne(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		cfg := Config{HUDFailureMultiplier: 0}
		require.NoError(t, cfg.Validate())
		assert.Equal(t, 1, cfg.HUDFailureMultiplier,
			"Validate must clamp HUDFailureMultiplier=0 to 1")
	})
	t.Run("negative", func(t *testing.T) {
		cfg := Config{HUDFailureMultiplier: -3}
		require.NoError(t, cfg.Validate())
		assert.Equal(t, 1, cfg.HUDFailureMultiplier,
			"Validate must clamp negative HUDFailureMultiplier to 1")
	})
}

// Validate() must clamp negative HUDFailureBaseCapacity to 0 for callers
// that construct Config programmatically (bypassing ConfigFromEnv's clamp).
func TestConfig_Validate_HUDFailureBaseCapacityClampedBelowZero(t *testing.T) {
	cfg := Config{HUDFailureBaseCapacity: -7}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 0, cfg.HUDFailureBaseCapacity,
		"Validate must clamp negative HUDFailureBaseCapacity to 0")
}

// Validate() clamps negative MaxRunners (set by main.go after env parse).
func TestConfig_Validate_MaxRunnersNegativeClamped(t *testing.T) {
	cfg := Config{MaxRunners: -3}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 0, cfg.MaxRunners,
		"Validate must clamp negative MaxRunners to 0")
}

func TestConfig_Validate_MaxRunnersZeroPreserved(t *testing.T) {
	cfg := Config{MaxRunners: 0}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 0, cfg.MaxRunners,
		"Validate preserves MaxRunners=0 (means unlimited downstream)")
}

func TestConfig_Validate_MaxRunnersPositivePreserved(t *testing.T) {
	cfg := Config{MaxRunners: 42}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 42, cfg.MaxRunners,
		"Validate preserves positive MaxRunners")
}

// Validate must reject Enabled=true with no RunnerNodeFleet — silently
// falling back would land runner placeholders on the workflow pool, which
// is precisely what the split-fleet config is here to prevent.
func TestConfig_Validate_EnabledWithoutRunnerNodeFleet_Errors(t *testing.T) {
	cfg := Config{Enabled: true, RunnerNodeFleet: ""}
	err := cfg.Validate()
	require.Error(t, err,
		"Validate must error when Enabled=true and RunnerNodeFleet is empty")
	assert.Contains(t, err.Error(), "CAPACITY_AWARE_RUNNER_NODE_FLEET",
		"error message must name the missing env var")
}

// Validate must accept Enabled=true when RunnerNodeFleet is set.
func TestConfig_Validate_EnabledWithRunnerNodeFleet_OK(t *testing.T) {
	cfg := Config{Enabled: true, RunnerNodeFleet: "c7i-runner"}
	assert.NoError(t, cfg.Validate(),
		"Validate must succeed when Enabled=true and RunnerNodeFleet is set")
}

// Validate must NOT error when capacity-aware mode is disabled, even with
// no RunnerNodeFleet — the config field is simply unused.
func TestConfig_Validate_DisabledWithoutRunnerNodeFleet_OK(t *testing.T) {
	cfg := Config{Enabled: false, RunnerNodeFleet: ""}
	assert.NoError(t, cfg.Validate(),
		"Validate must succeed when capacity-aware mode is disabled, regardless of RunnerNodeFleet")
}

// ConfigFromEnv loads CAPACITY_AWARE_RUNNER_NODE_FLEET into RunnerNodeFleet
// (separate from CAPACITY_AWARE_NODE_FLEET, which loads into NodeFleet).
func TestConfigFromEnv_RunnerNodeFleet_LoadedSeparately(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_NODE_FLEET":        "g4dn",
		"CAPACITY_AWARE_RUNNER_NODE_FLEET": "c7i-runner",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, "g4dn", cfg.NodeFleet,
		"NodeFleet (workflow pool) loaded from CAPACITY_AWARE_NODE_FLEET")
	assert.Equal(t, "c7i-runner", cfg.RunnerNodeFleet,
		"RunnerNodeFleet (runner pool) loaded from CAPACITY_AWARE_RUNNER_NODE_FLEET")
}

// Whitespace must be trimmed from CAPACITY_AWARE_RUNNER_NODE_FLEET, matching
// the behavior of CAPACITY_AWARE_NODE_FLEET.
func TestConfigFromEnv_RunnerNodeFleet_WhitespaceTrimmed(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_RUNNER_NODE_FLEET": "  c7i-runner  ",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, "c7i-runner", cfg.RunnerNodeFleet)
}

// The three sharding env vars must round-trip into ConfigFromEnv with sane
// values intact (no clamping when the values are already in-range).
func TestConfigFromEnv_Sharding_AllSet(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_CLUSTER_INDEX":         "1",
		"CAPACITY_AWARE_CLUSTER_COUNT":         "3",
		"CAPACITY_AWARE_AGE_THRESHOLD_SECONDS": "900",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 1, cfg.ClusterIndex)
	assert.Equal(t, 3, cfg.ClusterCount)
	assert.Equal(t, 900, cfg.AgeThresholdSeconds)
}

// Defaults disable sharding: ClusterCount=1 and AgeThresholdSeconds=0
// together short-circuit the two-call HUD path.
func TestConfigFromEnv_Sharding_Defaults(t *testing.T) {
	unsetEnvs(t, []string{
		"CAPACITY_AWARE_CLUSTER_INDEX",
		"CAPACITY_AWARE_CLUSTER_COUNT",
		"CAPACITY_AWARE_AGE_THRESHOLD_SECONDS",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 0, cfg.ClusterIndex, "default ClusterIndex")
	assert.Equal(t, 1, cfg.ClusterCount, "default ClusterCount disables sharding")
	assert.Equal(t, 0, cfg.AgeThresholdSeconds, "default AgeThresholdSeconds disables sharding")
}

// ClusterCount < 1 must clamp to 1 — downstream integer division would
// panic on zero and would slice nonsensically on negatives.
func TestConfigFromEnv_ClusterCount_BelowOneClampedToOne(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		setEnvs(t, map[string]string{"CAPACITY_AWARE_CLUSTER_COUNT": "0"})
		cfg := ConfigFromEnv()
		assert.Equal(t, 1, cfg.ClusterCount)
	})
	t.Run("negative", func(t *testing.T) {
		setEnvs(t, map[string]string{"CAPACITY_AWARE_CLUSTER_COUNT": "-3"})
		cfg := ConfigFromEnv()
		assert.Equal(t, 1, cfg.ClusterCount)
	})
}

// Negative ClusterIndex must clamp to 0 — it is used as an unsigned offset
// into the per-cluster slice arithmetic.
func TestConfigFromEnv_ClusterIndex_NegativeClampedToZero(t *testing.T) {
	setEnvs(t, map[string]string{"CAPACITY_AWARE_CLUSTER_INDEX": "-2"})
	cfg := ConfigFromEnv()
	assert.Equal(t, 0, cfg.ClusterIndex)
}

// ClusterIndex must always be < ClusterCount, otherwise the slice math
// would assign capacity to a non-existent cluster slot. Clamp to last.
func TestConfigFromEnv_ClusterIndex_GreaterEqualClusterCountClamped(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_CLUSTER_INDEX": "5",
		"CAPACITY_AWARE_CLUSTER_COUNT": "3",
	})
	cfg := ConfigFromEnv()
	assert.Equal(t, 3, cfg.ClusterCount)
	assert.Equal(t, 2, cfg.ClusterIndex, "ClusterIndex must clamp to ClusterCount-1")
}

// Negative AgeThresholdSeconds must clamp to 0 — used as a divisor and
// fed straight to HUD as a non-negative integer.
func TestConfigFromEnv_AgeThresholdSeconds_NegativeClampedToZero(t *testing.T) {
	setEnvs(t, map[string]string{"CAPACITY_AWARE_AGE_THRESHOLD_SECONDS": "-60"})
	cfg := ConfigFromEnv()
	assert.Equal(t, 0, cfg.AgeThresholdSeconds)
}

// HUD threshold parameter is minute-granular; any positive sub-60-second
// value rounds down to 0 minutes and would make every cluster claim 100%
// of the queue as aged (the inverse of sharding). Clamp such values up
// to 60. Zero stays zero — it is the explicit "sharding disabled" signal.
func TestClampShardingFields_AgeThresholdSubMinuteClampsToSixty(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"one", 1, 60},
		{"thirty", 30, 60},
		{"fifty-nine", 59, 60},
		{"zero-stays-zero", 0, 0},
		{"sixty-unchanged", 60, 60},
		{"nine-hundred-unchanged", 900, 900},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{AgeThresholdSeconds: tc.in}
			clampShardingFields(&cfg)
			assert.Equal(t, tc.want, cfg.AgeThresholdSeconds)
		})
	}
}

// Validate() must apply the same sharding clamps for callers that
// construct Config programmatically (bypassing ConfigFromEnv).
func TestConfig_Validate_ShardingClamps(t *testing.T) {
	cfg := Config{
		ClusterCount:        0,
		ClusterIndex:        -7,
		AgeThresholdSeconds: -1,
	}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 1, cfg.ClusterCount)
	assert.Equal(t, 0, cfg.ClusterIndex)
	assert.Equal(t, 0, cfg.AgeThresholdSeconds)

	cfg = Config{ClusterCount: 2, ClusterIndex: 5}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 2, cfg.ClusterCount)
	assert.Equal(t, 1, cfg.ClusterIndex, "Validate must clamp ClusterIndex to ClusterCount-1")
}

func TestConfigFromEnv_Multipliers_AllSet(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_FRESH_MULTIPLIER": "0.5",
		"CAPACITY_AWARE_AGED_MULTIPLIER":  "1.5",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 0.5, cfg.FreshMultiplier)
	assert.Equal(t, 1.5, cfg.AgedMultiplier)
}

func TestConfigFromEnv_Multipliers_Defaults(t *testing.T) {
	unsetEnvs(t, []string{
		"CAPACITY_AWARE_FRESH_MULTIPLIER",
		"CAPACITY_AWARE_AGED_MULTIPLIER",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 1.0, cfg.FreshMultiplier)
	assert.Equal(t, 1.0, cfg.AgedMultiplier)
}

func TestClampMultipliers_Negative(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_FRESH_MULTIPLIER": "-0.5",
		"CAPACITY_AWARE_AGED_MULTIPLIER":  "-2",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 0.0, cfg.FreshMultiplier)
	assert.Equal(t, 0.0, cfg.AgedMultiplier)
}

func TestClampMultipliers_NaN(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_FRESH_MULTIPLIER": "NaN",
		"CAPACITY_AWARE_AGED_MULTIPLIER":  "NaN",
	})

	cfg := ConfigFromEnv()

	assert.False(t, math.IsNaN(cfg.FreshMultiplier))
	assert.Equal(t, 1.0, cfg.FreshMultiplier)
	assert.Equal(t, 1.0, cfg.AgedMultiplier)
}

func TestClampMultipliers_PositiveInf(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_FRESH_MULTIPLIER": "+Inf",
		"CAPACITY_AWARE_AGED_MULTIPLIER":  "+Inf",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 1.0, cfg.FreshMultiplier)
	assert.Equal(t, 1.0, cfg.AgedMultiplier)
}

func TestClampMultipliers_Invalid(t *testing.T) {
	setEnvs(t, map[string]string{
		"CAPACITY_AWARE_FRESH_MULTIPLIER": "not-a-float",
		"CAPACITY_AWARE_AGED_MULTIPLIER":  "garbage",
	})

	cfg := ConfigFromEnv()

	assert.Equal(t, 1.0, cfg.FreshMultiplier)
	assert.Equal(t, 1.0, cfg.AgedMultiplier)
}

func TestClampMultipliers_TooLarge(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want float64
	}{
		{"scientific", "1e10", 1000},
		{"fifteen-hundred", "1500", 1000},
		{"ten-thousand", "10000", 1000},
		{"at-cap", "1000", 1000},
		{"below-cap", "999", 999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEnvs(t, map[string]string{
				"CAPACITY_AWARE_FRESH_MULTIPLIER": tc.in,
				"CAPACITY_AWARE_AGED_MULTIPLIER":  tc.in,
			})

			cfg := ConfigFromEnv()

			assert.Equal(t, tc.want, cfg.FreshMultiplier)
			assert.Equal(t, tc.want, cfg.AgedMultiplier)
		})
	}
}
