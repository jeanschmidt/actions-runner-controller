package capacity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/actions/actions-runner-controller/cmd/ghalistener/metrics"
)

// newTestMonitor creates a Monitor wired to a fake K8s client (with
// working DeleteCollection) and an optional HUD test server. If hudRows
// is nil, no HUD server is created and the monitor falls back to
// proactiveCapacity only.
func newTestMonitor(t *testing.T, cfg Config, hudRows []QueuedJobsForRunner) (*Monitor, *fake.Clientset, *atomic.Int32) {
	t.Helper()
	if cfg.Namespace == "" {
		cfg.Namespace = "test-ns"
	}
	if cfg.ScaleSetName == "" {
		cfg.ScaleSetName = "test-sset"
	}
	if cfg.FreshMultiplier == 0 {
		cfg.FreshMultiplier = 1.0
	}
	if cfg.AgedMultiplier == 0 {
		cfg.AgedMultiplier = 1.0
	}
	cs := newFakeClientset()

	var maxRunnersVal atomic.Int32
	setMax := func(v int) { maxRunnersVal.Store(int32(v)) }

	logger := discardLogger
	listenerID := "test-listener"

	m := &Monitor{
		config: cfg,
		placeholders: NewPlaceholderManager(
			cs, cfg.Namespace, listenerID, cfg, logger,
		),
		clientset:     cs,
		setMaxRunners: setMax,
		logger:        logger.With("component", "capacity-monitor"),
		recorder:      metrics.DiscardCapacity,
	}

	if hudRows != nil {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(hudRows)
		}))
		t.Cleanup(srv.Close)

		m.hudClient = NewHUDClient(srv.URL, "test")
		cfg.HUDAPIToken = "test"
		m.config = cfg
	}

	return m, cs, &maxRunnersVal
}

func countPods(t *testing.T, cs *fake.Clientset, ns string) int {
	t.Helper()
	pods, err := cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	return len(pods.Items)
}

func TestReconcile_ZeroQueued_CreatesProactiveCapacity(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         10,
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// 3 pairs = 6 pods.
	assert.Equal(t, 6, countPods(t, cs, "test-ns"))
	// No running pairs (fake client pods start with empty phase), so
	// setMaxRunners(min(0+0, 10)) = 0.
	assert.Equal(t, int32(0), maxVal.Load())
}

func TestReconcile_QueuedJobs_AddsToProactiveCapacity(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 5},
	}
	cfg := Config{
		ProactiveCapacity:  2,
		MaxRunners:         20,
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, hudRows)

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// desired = proactive(2) + queued(5) = 7 pairs = 14 pods.
	assert.Equal(t, 14, countPods(t, cs, "test-ns"))
	// No running pairs/runners yet, so capacity = 0.
	assert.Equal(t, int32(0), maxVal.Load())
}

func TestReconcile_MaxRunnersCap(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 50},
	}
	cfg := Config{
		ProactiveCapacity:  5,
		MaxRunners:         10,
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, hudRows)

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// desired = min(5+50, 10) = 10 pairs = 20 pods.
	assert.Equal(t, 20, countPods(t, cs, "test-ns"))
	// No running pairs/runners, so reported capacity = 0 (capped at MaxRunners=10).
	assert.Equal(t, int32(0), maxVal.Load())
}

func TestReconcile_ScaleDown_PrefersPendingDeletion(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  5,
		MaxRunners:         20,
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	ctx := context.Background()

	// First reconcile creates 5 pairs.
	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)
	assert.Equal(t, 10, countPods(t, cs, "test-ns"))

	// Make 2 pairs Running, leave 3 Pending.
	pairs, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	runningCount := 0
	for slotID := range pairs {
		if runningCount < 2 {
			setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodRunning)
			runningCount++
		} else {
			setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodPending)
		}
	}

	// Scale down to 2 pairs.
	m.config.ProactiveCapacity = 2
	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)

	// Should have deleted EXACTLY 3 pairs (the 3 Pending ones), not more
	// (regression test for adjustPairs double-delete counter bug).
	assert.Equal(t, 4, countPods(t, cs, "test-ns"),
		"must delete exactly 3 pairs (the Pending ones), leaving the 2 Running pairs")

	// Verify the remaining pods are the Running ones.
	remaining, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	assert.Len(t, remaining, 2, "exactly 2 pairs remain")
	for _, pair := range remaining {
		assert.True(t, pair.BothRunning(), "remaining pairs should be running")
	}
	// 2 running pairs, 0 running runners, capped at MaxRunners=20.
	assert.Equal(t, int32(2), maxVal.Load())
}

// Regression test for the adjustPairs double-delete bug: when scaling
// from 5 to 2 with all pairs Pending, ensure exactly 3 are deleted —
// not over-deleted (the bug double-counted slots in pass 2 when pass 1
// had already deleted them).
func TestAdjustPairs_NoDoubleDelete_AllPending(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  5,
		MaxRunners:         20,
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)
	ctx := context.Background()

	// Create 5 pairs, all Pending.
	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)
	pairs, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	for slotID := range pairs {
		setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodPending)
	}
	assert.Equal(t, 10, countPods(t, cs, "test-ns"))

	// Scale down to 2 pairs.
	m.config.ProactiveCapacity = 2
	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)

	// EXACTLY 3 pairs should be deleted (6 pods removed -> 4 remaining).
	// If the double-delete bug returned, fewer (or wrong) pairs would
	// remain because pass 2 would skip slots already deleted by pass 1
	// without correctly accounting for them.
	assert.Equal(t, 4, countPods(t, cs, "test-ns"),
		"exactly 3 pairs deleted, 2 remain")
}

func TestReconcile_SetMaxRunners_CapAtMaxRunners(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  2,
		MaxRunners:         5,
		ScaleSetName:       "test-sset",
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	ctx := context.Background()

	// Create some "real" ephemeral runner pods.
	for i := 0; i < 3; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "runner-" + string(rune('a'+i)),
				Namespace: "test-ns",
				Labels: map[string]string{
					"actions-ephemeral-runner": "True",
					labelScaleSet:              "test-sset",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "runner", Image: "runner:latest"}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		_, err := cs.CoreV1().Pods("test-ns").Create(ctx, pod, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	// Their job pods are scheduled (Running), so they count as secured capacity.
	createJobPods(t, cs, "test-ns", []string{"runner-a", "runner-b", "runner-c"}, corev1.PodRunning, "test-sset")

	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)

	// Make placeholder pairs Running.
	pairs, _ := m.placeholders.ListPairs(ctx)
	for slotID := range pairs {
		setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodRunning)
	}

	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)

	// capacity = min(scheduledRunners(3) + runningPairs(2), maxRunners(5)) = 5.
	assert.Equal(t, int32(5), maxVal.Load())
}

func TestReconcile_HUDAPIFailure_FallsBackToProactiveTimesMultiplier(t *testing.T) {
	// HUD server returns 500.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := Config{
		ProactiveCapacity:    3,
		HUDFailureMultiplier: 3,
		MaxRunners:           20,
		ScaleSetLabels:       []string{"linux.2xlarge"},
		HUDAPIToken:          "test",
		PlaceholderTimeout:   5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)

	m.hudClient = NewHUDClient(srv.URL, "test")

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// HUD failure -> over-provision: ProactiveCapacity(3) * HUDFailureMultiplier(3)
	// = 9 pairs = 18 pods. Less info about queue depth means lean toward more
	// capacity; outer caps still bound the absolute blast radius.
	assert.Equal(t, 18, countPods(t, cs, "test-ns"))
	// No running pairs, so capacity = 0 (still capped at MaxRunners=20).
	assert.Equal(t, int32(0), maxVal.Load())
}

// With multiplier=1, the HUD-failure fallback equals ProactiveCapacity alone.
func TestReconcile_HUDAPIFailure_MultiplierOne(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := Config{
		ProactiveCapacity:    3,
		HUDFailureMultiplier: 1,
		MaxRunners:           20,
		ScaleSetLabels:       []string{"linux.2xlarge"},
		HUDAPIToken:          "test",
		PlaceholderTimeout:   5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)
	m.hudClient = NewHUDClient(srv.URL, "test")

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// 3 * 1 = 3 pairs = 6 pods.
	assert.Equal(t, 6, countPods(t, cs, "test-ns"))
}

// MaxRunners must clamp the multiplier-amplified fallback so a misconfigured
// multiplier cannot exceed the hard runner cap.
func TestReconcile_HUDAPIFailure_MultiplierClampedByMaxRunners(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := Config{
		ProactiveCapacity:    5,
		HUDFailureMultiplier: 4,
		MaxRunners:           10,
		ScaleSetLabels:       []string{"linux.2xlarge"},
		HUDAPIToken:          "test",
		PlaceholderTimeout:   5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)
	m.hudClient = NewHUDClient(srv.URL, "test")

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// 5 * 4 = 20 desired, clamped to MaxRunners=10 -> 10 pairs = 20 pods.
	assert.Equal(t, 20, countPods(t, cs, "test-ns"))
}

// When HUD is disabled by config (no token), the multiplier path must not
// trigger — only the proactive baseline applies.
func TestReconcile_HUDDisabled_MultiplierDoesNotApply(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:    3,
		HUDFailureMultiplier: 99,
		MaxRunners:           100,
		HUDAPIToken:          "",
		PlaceholderTimeout:   5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// HUD disabled -> hudFailed stays false -> desired = ProactiveCapacity(3) +
	// queuedJobs(0) = 3 pairs = 6 pods. Multiplier of 99 must not apply.
	assert.Equal(t, 6, countPods(t, cs, "test-ns"))
}

// HUD failure fallback formula:
//
//	desiredPairs = ProactiveCapacity * HUDFailureMultiplier + HUDFailureBaseCapacity
//
// 3 * 3 + 5 = 14 pairs.
func TestReconcile_HUDAPIFailure_FormulaIncludesBaseCapacity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := Config{
		ProactiveCapacity:      3,
		HUDFailureMultiplier:   3,
		HUDFailureBaseCapacity: 5,
		MaxRunners:             50,
		ScaleSetLabels:         []string{"linux.2xlarge"},
		HUDAPIToken:            "test",
		PlaceholderTimeout:     5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)
	m.hudClient = NewHUDClient(srv.URL, "test")

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// 3 * 3 + 5 = 14 pairs = 28 pods.
	assert.Equal(t, 28, countPods(t, cs, "test-ns"),
		"HUD-failure formula must be ProactiveCapacity*HUDFailureMultiplier+HUDFailureBaseCapacity")
}

// Core use case: HUDFailureBaseCapacity provides a fallback floor even
// when ProactiveCapacity is 0 (multiplier path alone would yield 0).
func TestReconcile_HUDAPIFailure_BaseCapacityWithProactiveZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := Config{
		ProactiveCapacity:      0,
		HUDFailureMultiplier:   3,
		HUDFailureBaseCapacity: 10,
		MaxRunners:             50,
		ScaleSetLabels:         []string{"linux.2xlarge"},
		HUDAPIToken:            "test",
		PlaceholderTimeout:     5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)
	m.hudClient = NewHUDClient(srv.URL, "test")

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// 0 * 3 + 10 = 10 pairs = 20 pods. Validates the motivating use case:
	// HUD-failure burst capacity without any always-on proactive baseline.
	assert.Equal(t, 20, countPods(t, cs, "test-ns"),
		"HUDFailureBaseCapacity must provide a floor when ProactiveCapacity=0")
}

// HUDFailureBaseCapacity contribution is still clamped by MaxRunners.
func TestReconcile_HUDAPIFailure_BaseCapacityClampedByMaxRunners(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := Config{
		ProactiveCapacity:      0,
		HUDFailureMultiplier:   1,
		HUDFailureBaseCapacity: 20,
		MaxRunners:             5,
		ScaleSetLabels:         []string{"linux.2xlarge"},
		HUDAPIToken:            "test",
		PlaceholderTimeout:     5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)
	m.hudClient = NewHUDClient(srv.URL, "test")

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// 0*1 + 20 = 20 desired, clamped to MaxRunners=5 -> 5 pairs = 10 pods.
	assert.Equal(t, 10, countPods(t, cs, "test-ns"),
		"HUDFailureBaseCapacity contribution must still be clamped by MaxRunners")
}

// Regression: HUDFailureBaseCapacity defaulting to 0 preserves the legacy
// fallback formula (ProactiveCapacity * HUDFailureMultiplier). Pins
// backward compatibility for existing deployments.
func TestReconcile_HUDAPIFailure_BaseCapacityDefaultZeroPreservesLegacy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := Config{
		ProactiveCapacity:      3,
		HUDFailureMultiplier:   3,
		HUDFailureBaseCapacity: 0,
		MaxRunners:             50,
		ScaleSetLabels:         []string{"linux.2xlarge"},
		HUDAPIToken:            "test",
		PlaceholderTimeout:     5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)
	m.hudClient = NewHUDClient(srv.URL, "test")

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// 3 * 3 + 0 = 9 pairs = 18 pods (matches legacy behavior).
	assert.Equal(t, 18, countPods(t, cs, "test-ns"),
		"HUDFailureBaseCapacity=0 must preserve the legacy ProactiveCapacity*HUDFailureMultiplier formula")
}

// On the HUD-healthy path, HUDFailureBaseCapacity must NOT contribute —
// the formula is the standard ProactiveCapacity + queuedJobs.
func TestReconcile_HUDHealthy_BaseCapacityDoesNotApply(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 2},
	}
	cfg := Config{
		ProactiveCapacity:      3,
		HUDFailureMultiplier:   3,
		HUDFailureBaseCapacity: 10,
		MaxRunners:             50,
		ScaleSetLabels:         []string{"linux.2xlarge"},
		PlaceholderTimeout:     5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, hudRows)

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())

	// HUD healthy -> desired = ProactiveCapacity(3) + queuedJobs(2) = 5 pairs
	// = 10 pods. HUDFailureBaseCapacity is NOT added on the healthy path.
	assert.Equal(t, 10, countPods(t, cs, "test-ns"),
		"HUDFailureBaseCapacity must NOT contribute when HUD is healthy")
}

func TestReconcile_IdempotentWhenAtDesired(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  2,
		MaxRunners:         10,
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)

	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())
	assert.Equal(t, 4, countPods(t, cs, "test-ns"))
	assert.Equal(t, int32(0), maxVal.Load(),
		"no running pairs after first reconcile -> capacity 0")

	// Second reconcile should not change anything.
	m.reconcileProvisioning(context.Background())
	m.reconcileReporting(context.Background())
	assert.Equal(t, 4, countPods(t, cs, "test-ns"))
	assert.Equal(t, int32(0), maxVal.Load(),
		"second reconcile preserves capacity 0")
}

// MaxRunners == 0 is a hard zero cap (operator drain), NOT "unlimited".
// The controller substitutes the unlimitedMaxRunners sentinel for an unset
// Spec.MaxRunners, so a literal 0 reaching the monitor is an explicit drain:
// create no placeholders and advertise 0 capacity to GitHub even while real
// runners are still running.
func TestReconcile_MaxRunnersZero_IsHardCap(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         0, // hard drain
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	ctx := context.Background()

	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)

	// Drained: headroom = 0 - 0 = 0, so no placeholders are created.
	assert.Equal(t, 0, countPods(t, cs, "test-ns"),
		"MaxRunners=0 must create 0 placeholders (hard drain)")
	// No capacity advertised.
	assert.Equal(t, int32(0), maxVal.Load())

	// Add real running runner pods: advertised capacity must still clamp to 0.
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 7, corev1.PodRunning, "runner")

	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)

	// capacity = min(runningRunners(7) + runningPairs(0), MaxRunners(0)) = 0.
	assert.Equal(t, int32(0), maxVal.Load(),
		"MaxRunners=0 must clamp reported capacity to 0 even with running runners")
	// Still no placeholder pairs (drained).
	pairs, _ := m.placeholders.ListPairs(ctx)
	assert.Empty(t, pairs, "MaxRunners=0 must keep the placeholder pool drained")
}

// MaxRunners == unlimitedMaxRunners is the "unlimited" sentinel the controller
// sets for an unset Spec.MaxRunners. All three MaxRunners clamps must be
// no-ops: placeholders are created per ProactiveCapacity+queued and the
// advertised capacity tracks running runners + running pairs uncapped.
func TestReconcile_MaxRunnersUnset_IsUnlimited(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         unlimitedMaxRunners, // unlimited sentinel
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	ctx := context.Background()

	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)

	// 3 pairs = 6 pods (unlimited -> no headroom clamp).
	assert.Equal(t, 6, countPods(t, cs, "test-ns"),
		"unlimited sentinel must NOT cap placeholders")
	// No running pairs yet -> capacity = 0 (not because of a cap).
	assert.Equal(t, int32(0), maxVal.Load())

	// Make all pairs Running and add some real runner pods.
	pairs, _ := m.placeholders.ListPairs(ctx)
	for slotID := range pairs {
		setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodRunning)
	}
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 7, corev1.PodRunning, "runner")
	// Their job pods are scheduled (Running), so they count as secured capacity.
	createJobPods(t, cs, "test-ns", jobPodNames("runner", 7), corev1.PodRunning, "test-sset")

	m.reconcileProvisioning(ctx)
	m.reconcileReporting(ctx)

	// capacity = scheduledRunners(7) + runningPairs(3) = 10, uncapped.
	assert.Equal(t, int32(10), maxVal.Load(),
		"unlimited sentinel must NOT cap reported capacity")
	// Still 3 pairs (proactive=3, no queued, no cap).
	pairs, _ = m.placeholders.ListPairs(ctx)
	assert.Len(t, pairs, 3)
}

// TestReconcile_StuckRunnerNotCountedAsCapacity locks in the core fix: a runner
// pod that is Running but whose job pod is stuck Pending (e.g. no schedulable
// GPU node) must NOT be advertised as capacity — otherwise the listener keeps
// acquiring jobs it cannot fulfil. Only runners whose job pod is Running count.
func TestReconcile_StuckRunnerNotCountedAsCapacity(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  0, // no placeholders — isolate the runner term
		MaxRunners:         unlimitedMaxRunners,
		ScaleSetName:       "test-sset",
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	ctx := context.Background()

	// 3 Running runner pods: one scheduled, two GPU-starved (job pod Pending).
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 3, corev1.PodRunning, "runner")
	createJobPods(t, cs, "test-ns", []string{"runner-0"}, corev1.PodRunning, "test-sset")
	createJobPods(t, cs, "test-ns", []string{"runner-1", "runner-2"}, corev1.PodPending, "test-sset")

	m.reconcileReporting(ctx)

	// Only runner-0 (job pod Running) counts. The two stuck runners do not.
	assert.Equal(t, int32(1), maxVal.Load(),
		"runners whose job pod is Pending must not advertise phantom capacity")
}

// TestReconcile_JobPodMissingLabel_FallsBackToNamespaceList verifies the
// deploy-ordering safety net: when job pods lack labelJobScaleSet (label not yet
// deployed), the scoped List is empty but the namespace-wide fallback still finds
// them, so capacity is not under-reported to zero.
func TestReconcile_JobPodMissingLabel_FallsBackToNamespaceList(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  0,
		MaxRunners:         unlimitedMaxRunners,
		ScaleSetName:       "test-sset",
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	ctx := context.Background()

	createRealRunnerPods(t, cs, "test-ns", "test-sset", 2, corev1.PodRunning, "runner")
	// Unlabeled job pods (scaleSet="") — the scoped List won't match them.
	createJobPods(t, cs, "test-ns", jobPodNames("runner", 2), corev1.PodRunning, "")

	m.reconcileReporting(ctx)

	assert.Equal(t, int32(2), maxVal.Load(),
		"fallback must count job pods even when the scale-set label is absent")
}

func TestRunLoop_CancellationCleansUp(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   1,
		MaxRunners:          5,
		RecalculateInterval: 100 * time.Millisecond,
		ReportInterval:      50 * time.Millisecond,
		PlaceholderTimeout:  5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := m.Run(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// After Run returns, all placeholders should be cleaned up.
	pods, listErr := cs.CoreV1().Pods("test-ns").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, listErr)
	assert.Empty(t, pods.Items, "all placeholders cleaned up on shutdown")
}

func TestReporter_IndependentOfProvisioner(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         10,
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	ctx := context.Background()

	// Provisioner creates 3 pairs (all start Pending in fake client).
	m.reconcileProvisioning(ctx)
	assert.Equal(t, 6, countPods(t, cs, "test-ns"))

	// Reporter runs — no Running pairs yet, capacity stays 0.
	m.reconcileReporting(ctx)
	assert.Equal(t, int32(0), maxVal.Load())

	// Simulate pods becoming Running (e.g., Karpenter provisioned nodes).
	pairs, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	for slotID := range pairs {
		setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodRunning)
	}

	// Reporter picks up Running pairs WITHOUT provisioner running again.
	m.reconcileReporting(ctx)
	assert.Equal(t, int32(3), maxVal.Load(),
		"reporter independently detects Running pairs")
}

func TestRetryWithBackoff_SucceedsOnRetry(t *testing.T) {
	attempts := 0
	err := retryWithBackoff(context.Background(), discardLogger, "test-op", 3, func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("transient error")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestRetryWithBackoff_ExhaustsRetries(t *testing.T) {
	attempts := 0
	err := retryWithBackoff(context.Background(), discardLogger, "test-op", 2, func() error {
		attempts++
		return fmt.Errorf("persistent error")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, attempts) // initial + 2 retries
	assert.Contains(t, err.Error(), "persistent error")
}

func TestRetryWithBackoff_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := retryWithBackoff(ctx, discardLogger, "test-op", 5, func() error {
		return fmt.Errorf("should not matter")
	})
	// First attempt runs (no backoff wait), fails, then backoff select sees ctx.Done()
	assert.ErrorIs(t, err, context.Canceled)
}

// TestProvisioner_BrokenPair_TriggersRecreation simulates a broken slot
// (only the runner pod present, workflow pod has been deleted/evicted) and
// asserts that the provisioner deletes the orphan pod, excludes the slot
// from currentPairs in the SAME cycle, and creates a fresh full pair to
// fill the freed slot — restoring the pre-warmed capacity.
//
// Without CleanupBroken, the slot would count as healthy and the provisioner
// would think it was already at desired, leaving capacity permanently low.
func TestProvisioner_BrokenPair_TriggersRecreation(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  5,
		MaxRunners:         20,
		PlaceholderTimeout: 5 * time.Minute,
	}
	rec := newFakeCapacityRecorder()
	m, cs, _ := newTestMonitorWithRecorder(t, cfg, rec)
	ctx := context.Background()

	// Initial reconcile creates 5 healthy pairs (10 pods).
	m.reconcileProvisioning(ctx)
	assert.Equal(t, 10, countPods(t, cs, "test-ns"),
		"5 healthy pairs after first reconcile")

	// Break one slot by deleting just its workflow pod (simulating
	// eviction/crash of the workflow placeholder).
	pairs, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	require.Len(t, pairs, 5)
	var brokenSlotID string
	for slotID := range pairs {
		brokenSlotID = slotID
		break
	}
	wfPods, err := cs.CoreV1().Pods("test-ns").List(ctx, metav1.ListOptions{
		LabelSelector: labelPlaceholderID + "=" + brokenSlotID + "," +
			labelPlaceholderRole + "=" + rolePlaceholderWorkflow,
	})
	require.NoError(t, err)
	require.Len(t, wfPods.Items, 1)
	require.NoError(t, cs.CoreV1().Pods("test-ns").Delete(
		ctx, wfPods.Items[0].Name, metav1.DeleteOptions{},
	))

	// 4 pairs healthy + 1 surviving runner pod = 9 pods.
	assert.Equal(t, 9, countPods(t, cs, "test-ns"),
		"after break: 4 healthy pairs + 1 orphan runner pod")

	// Run provisioning again. The broken slot should be detected,
	// its surviving runner pod deleted, and a fresh full pair created
	// in the SAME cycle (because CleanupBroken updates pairs and
	// currentPairs in-place before adjustPairs runs).
	m.reconcileProvisioning(ctx)

	// End state: 5 healthy pairs (= 10 pods).
	assert.Equal(t, 10, countPods(t, cs, "test-ns"),
		"5 healthy pairs restored in the same cycle")

	// The original broken slot must no longer exist (it was deleted).
	finalPairs, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	assert.Len(t, finalPairs, 5, "exactly 5 pairs in steady state")
	assert.NotContains(t, finalPairs, brokenSlotID,
		"broken slot must be gone (replaced by a fresh slot)")
	for slotID, pair := range finalPairs {
		assert.NotNilf(t, pair.RunnerPod, "slot %s must have runner pod", slotID)
		assert.NotNilf(t, pair.WorkflowPod, "slot %s must have workflow pod", slotID)
	}

	// Metrics: one broken-success delete recorded, plus one new pair create.
	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Equal(t, 1, rec.incPairDeletesCalls[deleteReasonBroken+":"+resultSuccess],
		"one broken delete recorded as success")
	assert.Equal(t, 0, rec.incPairDeletesCalls[deleteReasonBroken+":"+resultError],
		"no broken delete failures")
	// Pair creates: 5 (initial) + 1 (replacement). Cycles run twice.
	assert.Equal(t, 6, rec.incPairCreatesCalls[resultSuccess],
		"5 initial creates + 1 replacement create")
}

// TestProvisioner_BrokenPair_DeleteFailure simulates a DeletePair
// failure during broken-pair cleanup. The cycle must not crash, the
// slot must still be excluded from currentPairs (so the next cycle
// re-detects it), and a broken-error metric must be recorded.
func TestProvisioner_BrokenPair_DeleteFailure(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         20,
		PlaceholderTimeout: 5 * time.Minute,
	}
	rec := newFakeCapacityRecorder()
	m, cs, _ := newTestMonitorWithRecorder(t, cfg, rec)
	ctx := context.Background()

	// Initial reconcile creates 3 healthy pairs.
	m.reconcileProvisioning(ctx)
	require.Equal(t, 6, countPods(t, cs, "test-ns"))

	// Break one slot.
	pairs, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	var brokenSlotID string
	for slotID := range pairs {
		brokenSlotID = slotID
		break
	}
	wfPods, err := cs.CoreV1().Pods("test-ns").List(ctx, metav1.ListOptions{
		LabelSelector: labelPlaceholderID + "=" + brokenSlotID + "," +
			labelPlaceholderRole + "=" + rolePlaceholderWorkflow,
	})
	require.NoError(t, err)
	require.NoError(t, cs.CoreV1().Pods("test-ns").Delete(
		ctx, wfPods.Items[0].Name, metav1.DeleteOptions{},
	))

	// Inject a delete-collection failure that targets only the broken
	// slot (so the second-cycle replacement-create + the orphan-runner
	// cleanup the next cycle does aren't blocked by the same reactor).
	cs.PrependReactor("delete-collection", "pods",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			dca := action.(k8stesting.DeleteCollectionActionImpl)
			sel := dca.GetListOptions().LabelSelector
			if strings.Contains(sel, labelPlaceholderID+"="+brokenSlotID) {
				return true, nil, fmt.Errorf("simulated delete failure")
			}
			return false, nil, nil
		},
	)

	// Run the provisioner — must not crash even though delete fails.
	assert.NotPanics(t, func() { m.reconcileProvisioning(ctx) })

	// The broken slot must still have been excluded from currentPairs,
	// so the provisioner created a replacement pair: total existing pairs
	// = 4 (3 originals minus 1 still-broken + 1 new replacement).
	pairs, err = m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	assert.Len(t, pairs, 4,
		"4 pairs total: 2 untouched healthy + 1 still-broken (delete failed) + 1 new replacement")

	// Metrics: one broken-error delete recorded.
	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Equal(t, 0, rec.incPairDeletesCalls[deleteReasonBroken+":"+resultSuccess],
		"delete failed -> no broken success")
	assert.Equal(t, 1, rec.incPairDeletesCalls[deleteReasonBroken+":"+resultError],
		"one broken delete failure recorded")
}

// ---- capacity recorder wiring tests ----
//
// These tests assert that the monitor calls the right CapacityRecorder
// methods at the right times. They do NOT exhaustively test every
// metric — the metrics package has its own tests for that. The goal here
// is to catch wiring drift: if the call site moves or is dropped, these
// tests fail.

// fakeCapacityRecorder is a CapacityRecorder that records every call so
// tests can assert which methods were invoked and with what arguments.
type fakeCapacityRecorder struct {
	mu sync.Mutex

	proactiveCapacity      int
	maxBurstCapacity       int
	hudFailureBaseCapacity int
	hudEnabled             bool
	queuedJobs             int
	desiredPairs           int
	pairs                  int
	runningPairs           int
	advertisedMaxRunners   int

	// Map (role, phase) -> latest count.
	placeholderPods map[string]map[string]int

	// Map phase -> last successful time.
	lastSuccess map[string]time.Time

	// Counts of method invocations for assertions.
	setProactiveCapacityCalls      int
	setMaxBurstCapacityCalls       int
	setHUDFailureBaseCapacityCalls int
	setHUDEnabledCalls             int
	setQueuedJobsCalls             int
	setDesiredPairsCalls           int
	setPairsCalls                  int
	setRunningPairsCalls           int
	setPlaceholderPodsCalls        int
	setAdvertisedMaxRunnersCalls   int
	setReconcileLastSuccessCalls   map[string]int
	observeReconcileCalls          map[string]int
	observeHUDRequestCalls         map[string]int
	incHUDRequestsCalls            map[string]int
	incPairCreatesCalls            map[string]int
	incPairDeletesCalls            map[string]int // key: reason+":"+result
	incReconcileSkipsCalls         map[string]int
}

func newFakeCapacityRecorder() *fakeCapacityRecorder {
	return &fakeCapacityRecorder{
		placeholderPods:              make(map[string]map[string]int),
		lastSuccess:                  make(map[string]time.Time),
		setReconcileLastSuccessCalls: make(map[string]int),
		observeReconcileCalls:        make(map[string]int),
		observeHUDRequestCalls:       make(map[string]int),
		incHUDRequestsCalls:          make(map[string]int),
		incPairCreatesCalls:          make(map[string]int),
		incPairDeletesCalls:          make(map[string]int),
		incReconcileSkipsCalls:       make(map[string]int),
	}
}

func (f *fakeCapacityRecorder) SetProactiveCapacity(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proactiveCapacity = v
	f.setProactiveCapacityCalls++
}
func (f *fakeCapacityRecorder) SetMaxBurstCapacity(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.maxBurstCapacity = v
	f.setMaxBurstCapacityCalls++
}
func (f *fakeCapacityRecorder) SetHUDFailureBaseCapacity(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hudFailureBaseCapacity = v
	f.setHUDFailureBaseCapacityCalls++
}
func (f *fakeCapacityRecorder) SetHUDEnabled(b bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hudEnabled = b
	f.setHUDEnabledCalls++
}
func (f *fakeCapacityRecorder) SetQueuedJobs(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queuedJobs = v
	f.setQueuedJobsCalls++
}
func (f *fakeCapacityRecorder) SetDesiredPairs(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.desiredPairs = v
	f.setDesiredPairsCalls++
}
func (f *fakeCapacityRecorder) SetPairs(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pairs = v
	f.setPairsCalls++
}
func (f *fakeCapacityRecorder) SetRunningPairs(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runningPairs = v
	f.setRunningPairsCalls++
}
func (f *fakeCapacityRecorder) SetPlaceholderPods(role, phase string, v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.placeholderPods[role] == nil {
		f.placeholderPods[role] = make(map[string]int)
	}
	f.placeholderPods[role][phase] = v
	f.setPlaceholderPodsCalls++
}
func (f *fakeCapacityRecorder) SetAdvertisedMaxRunners(v int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advertisedMaxRunners = v
	f.setAdvertisedMaxRunnersCalls++
}
func (f *fakeCapacityRecorder) SetReconcileLastSuccess(phase string, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSuccess[phase] = t
	f.setReconcileLastSuccessCalls[phase]++
}
func (f *fakeCapacityRecorder) ObserveReconcileDuration(phase string, _ time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeReconcileCalls[phase]++
}
func (f *fakeCapacityRecorder) ObserveHUDRequest(result string, _ time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeHUDRequestCalls[result]++
}
func (f *fakeCapacityRecorder) IncHUDRequests(result string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.incHUDRequestsCalls[result]++
}
func (f *fakeCapacityRecorder) IncPairCreates(result string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.incPairCreatesCalls[result]++
}
func (f *fakeCapacityRecorder) IncPairDeletes(reason, result string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.incPairDeletesCalls[reason+":"+result]++
}
func (f *fakeCapacityRecorder) IncReconcileSkips(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.incReconcileSkipsCalls[reason]++
}

var _ metrics.CapacityRecorder = (*fakeCapacityRecorder)(nil)

// newTestMonitorWithRecorder is like newTestMonitor but injects a custom
// recorder so the test can assert metric calls. The fixture mirrors the
// shape of newTestMonitor exactly — only the recorder changes.
func newTestMonitorWithRecorder(
	t *testing.T,
	cfg Config,
	rec metrics.CapacityRecorder,
) (*Monitor, *fake.Clientset, *atomic.Int32) {
	t.Helper()
	m, cs, val := newTestMonitor(t, cfg, nil)
	m.recorder = rec
	return m, cs, val
}

// TestProvisioner_RecorderWiring exercises one full provisioner cycle and
// asserts the gauges + last-success-timestamp landed.
func TestProvisioner_RecorderWiring(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         10,
		PlaceholderTimeout: 5 * time.Minute,
	}
	rec := newFakeCapacityRecorder()
	m, _, _ := newTestMonitorWithRecorder(t, cfg, rec)

	m.reconcileProvisioning(context.Background())

	rec.mu.Lock()
	defer rec.mu.Unlock()

	assert.Equal(t, 3, rec.desiredPairs, "desired pairs should be set to ProactiveCapacity")
	assert.GreaterOrEqual(t, rec.setDesiredPairsCalls, 1)

	assert.Equal(t, 0, rec.pairs, "no pairs existed before this cycle")
	assert.GreaterOrEqual(t, rec.setPairsCalls, 1)

	assert.Equal(t, 1, rec.setReconcileLastSuccessCalls[reconcilePhaseProvisioner],
		"provisioner success timestamp must be recorded once on the success path")
	assert.False(t, rec.lastSuccess[reconcilePhaseProvisioner].IsZero())

	// Duration histogram is observed on every cycle (success or fail).
	assert.Equal(t, 1, rec.observeReconcileCalls[reconcilePhaseProvisioner])

	// Created 3 pairs successfully.
	assert.Equal(t, 3, rec.incPairCreatesCalls[resultSuccess])
}

// TestReporter_RecorderWiring exercises one reporter cycle and asserts
// the advertised-max-runners gauge + last-success-timestamp landed.
func TestReporter_RecorderWiring(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  2,
		MaxRunners:         10,
		PlaceholderTimeout: 5 * time.Minute,
	}
	rec := newFakeCapacityRecorder()
	m, _, _ := newTestMonitorWithRecorder(t, cfg, rec)

	m.reconcileReporting(context.Background())

	rec.mu.Lock()
	defer rec.mu.Unlock()

	assert.Equal(t, 0, rec.advertisedMaxRunners,
		"no running pairs/runners yet, capacity is 0")
	assert.GreaterOrEqual(t, rec.setAdvertisedMaxRunnersCalls, 1)

	assert.Equal(t, 1, rec.setReconcileLastSuccessCalls[reconcilePhaseReporter],
		"reporter success timestamp must be recorded once on the success path")
	assert.False(t, rec.lastSuccess[reconcilePhaseReporter].IsZero())

	assert.Equal(t, 1, rec.observeReconcileCalls[reconcilePhaseReporter])
}

// TestProvisioner_ListPairsError_RecordsSkip simulates a list-pairs error
// in the provisioner and asserts the skip counter is incremented and
// the success timestamp is NOT advanced.
func TestProvisioner_ListPairsError_RecordsSkip(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  1,
		MaxRunners:         10,
		PlaceholderTimeout: 5 * time.Minute,
	}
	rec := newFakeCapacityRecorder()
	m, cs, _ := newTestMonitorWithRecorder(t, cfg, rec)

	// Make every Pods().List() call fail. retryWithBackoff will burn
	// through all attempts, then the provisioner takes the skip path.
	cs.PrependReactor("list", "pods",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("synthetic list pods error")
		},
	)

	// Use a context with a short deadline so the retry backoffs (1s, 2s, 4s)
	// are aborted quickly via ctx.Done() instead of running for 7+ seconds.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	m.reconcileProvisioning(ctx)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	assert.Equal(t, 1, rec.incReconcileSkipsCalls[skipReasonProvisionerListPairs],
		"list-pairs error must record a provisioner_list_pairs skip")
	assert.Equal(t, 0, rec.setReconcileLastSuccessCalls[reconcilePhaseProvisioner],
		"success timestamp must NOT be set on the skip path")
	// Duration is still observed even on the skip path.
	assert.Equal(t, 1, rec.observeReconcileCalls[reconcilePhaseProvisioner])
}

// TestProvisioner_RunnerCountError_RecordsSkip simulates a failure when
// counting real runner pods (Running/Pending phase) and asserts the
// provisioner skips the cycle: the skip counter is incremented, the
// success timestamp is NOT advanced, and no placeholder pairs are
// created (the headroom calculation never ran, so we must not over-
// create). This guards the doubling-the-cap regression that would
// happen if a list failure was silently treated as 0 runners.
func TestProvisioner_RunnerCountError_RecordsSkip(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         10,
		PlaceholderTimeout: 5 * time.Minute,
	}
	rec := newFakeCapacityRecorder()
	m, cs, _ := newTestMonitorWithRecorder(t, cfg, rec)

	// Fail ONLY the runner-count list calls (those use the
	// "actions-ephemeral-runner=True,..." label selector). Placeholder
	// list calls use a different selector and continue to succeed, so
	// the cycle reaches the runner-count step before erroring out.
	cs.PrependReactor("list", "pods",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			la, ok := action.(k8stesting.ListAction)
			if !ok {
				return false, nil, nil
			}
			sel := la.GetListRestrictions().Labels.String()
			if strings.Contains(sel, "actions-ephemeral-runner=True") {
				return true, nil, fmt.Errorf("synthetic count-runners error")
			}
			return false, nil, nil
		},
	)

	// Short deadline so retry backoffs (1s, 2s, 4s) abort quickly via ctx.Done().
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	m.reconcileProvisioning(ctx)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	assert.Equal(t, 1, rec.incReconcileSkipsCalls[skipReasonProvisionerListRunners],
		"runner-count error must record a provisioner_list_runners skip")
	assert.Equal(t, 0, rec.setReconcileLastSuccessCalls[reconcilePhaseProvisioner],
		"success timestamp must NOT be set on the skip path")
	// Duration is still observed even on the skip path.
	assert.Equal(t, 1, rec.observeReconcileCalls[reconcilePhaseProvisioner])
	// SetDesiredPairs must NOT be called — the cycle bailed out before
	// the headroom calculation.
	assert.Equal(t, 0, rec.setDesiredPairsCalls,
		"desiredPairs must not be set when the cycle is skipped")
	// No placeholder pairs were created (the adjustPairs step never ran).
	assert.Equal(t, 0, countPods(t, cs, "test-ns"),
		"no placeholders must be created when the cycle is skipped")
}

// ---- MaxBurstCapacity / MaxRunners headroom tests ----

// createRealRunnerPods creates n real EphemeralRunner pods for the scale set
// in the given phase. Mirrors the label shape used by countRunnersByPhaseWithRetry
// so the provisioner and reporter see them as "real".
func createRealRunnerPods(
	t *testing.T,
	cs *fake.Clientset,
	ns, scaleSetName string,
	n int,
	phase corev1.PodPhase,
	namePrefix string,
) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", namePrefix, i),
				Namespace: ns,
				Labels: map[string]string{
					"actions-ephemeral-runner": "True",
					labelScaleSet:              scaleSetName,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "runner", Image: "runner:latest"}},
			},
			Status: corev1.PodStatus{Phase: phase},
		}
		_, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
		require.NoError(t, err)
	}
}

// createJobPods creates a real job pod ("<runner>-workflow") for each named
// runner in the given phase. scaleSet, when non-empty, stamps labelJobScaleSet
// (exercising the scoped List path); "" leaves it unlabeled (fallback path).
func createJobPods(t *testing.T, cs *fake.Clientset, ns string, runnerNames []string, phase corev1.PodPhase, scaleSet string) {
	t.Helper()
	ctx := context.Background()
	for _, rn := range runnerNames {
		labels := map[string]string{}
		if scaleSet != "" {
			labels[labelJobScaleSet] = scaleSet
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rn + "-workflow",
				Namespace: ns,
				Labels:    labels,
			},
			Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "job", Image: "job:latest"}}},
			Status: corev1.PodStatus{Phase: phase},
		}
		_, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
		require.NoError(t, err)
	}
}

// jobPodNames returns "<prefix>-0"..."<prefix>-(n-1)", matching the runner pod
// names produced by createRealRunnerPods.
func jobPodNames(prefix string, n int) []string {
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return names
}

// MaxBurstCapacity = 0 means "no cap" — desiredPairs equals the proactive +
// queued sum, just like before the cap was introduced.
func TestReconcile_MaxBurstCapacity_ZeroIsNoCap(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 8},
	}
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         unlimitedMaxRunners, // unlimited so headroom can't interfere
		MaxBurstCapacity:   0,                   // unlimited
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, hudRows)

	m.reconcileProvisioning(context.Background())

	// desired = 3 + 8 = 11 pairs = 22 pods, no cap applied.
	assert.Equal(t, 22, countPods(t, cs, "test-ns"),
		"MaxBurstCapacity=0 must NOT cap placeholders")
}

// MaxBurstCapacity > 0 and desired exceeds it -> clamp to MaxBurstCapacity.
func TestReconcile_MaxBurstCapacity_ClampsWhenExceeded(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 50},
	}
	cfg := Config{
		ProactiveCapacity:  5,
		MaxRunners:         unlimitedMaxRunners, // unlimited so MaxBurstCapacity is the only cap
		MaxBurstCapacity:   7,
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, hudRows)

	m.reconcileProvisioning(context.Background())

	// desired = min(5+50, MaxBurstCapacity=7) = 7 pairs = 14 pods.
	assert.Equal(t, 14, countPods(t, cs, "test-ns"),
		"desired must be clamped to MaxBurstCapacity")
}

// MaxBurstCapacity > 0 but desired stays below the cap -> no clamp.
func TestReconcile_MaxBurstCapacity_UnclampedWhenBelow(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 2},
	}
	cfg := Config{
		ProactiveCapacity:  3,
		MaxRunners:         unlimitedMaxRunners, // unlimited so MaxBurstCapacity is the only cap
		MaxBurstCapacity:   20,
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, hudRows)

	m.reconcileProvisioning(context.Background())

	// desired = 3 + 2 = 5 (well below MaxBurstCapacity=20). 5 pairs = 10 pods.
	assert.Equal(t, 10, countPods(t, cs, "test-ns"),
		"desired below MaxBurstCapacity must remain unclamped")
}

// Both MaxRunners-headroom and MaxBurstCapacity active; MaxRunners-headroom is
// tighter (it leaves only 2 slots while MaxBurstCapacity allows 50) -> headroom wins.
func TestReconcile_MaxRunnersHeadroom_TighterThanBurst(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 100},
	}
	cfg := Config{
		ProactiveCapacity:  0,
		MaxRunners:         10,
		MaxBurstCapacity:   50,
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, hudRows)
	// 8 real runner pods consume the cap, leaving only 2 slots of headroom.
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 8, corev1.PodRunning, "runner-r")

	m.reconcileProvisioning(context.Background())

	// MaxRunners-headroom = 10 - 8 = 2; MaxBurstCapacity = 50.
	// desired = min(100, 2, 50) = 2 pairs = 4 pods (plus the 8 real pods = 12 total).
	pairs, err := m.placeholders.ListPairs(context.Background())
	require.NoError(t, err)
	assert.Len(t, pairs, 2,
		"MaxRunners-headroom (2) must win over MaxBurstCapacity (50)")
}

// Both MaxRunners-headroom and MaxBurstCapacity active; MaxBurstCapacity is
// the tighter cap (allows 4 while headroom would allow 50) -> burst wins.
func TestReconcile_MaxBurstCapacity_TighterThanHeadroom(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 100},
	}
	cfg := Config{
		ProactiveCapacity:  0,
		MaxRunners:         100, // headroom is wide open
		MaxBurstCapacity:   4,   // burst is the tight one
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, hudRows)
	// A few real runners exist but headroom (100-5=95) is still much larger than 4.
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 5, corev1.PodRunning, "runner-r")

	m.reconcileProvisioning(context.Background())

	// desired = min(100, 100-5=95, 4) = 4 pairs = 8 pods.
	pairs, err := m.placeholders.ListPairs(context.Background())
	require.NoError(t, err)
	assert.Len(t, pairs, 4,
		"MaxBurstCapacity (4) must win over MaxRunners-headroom (95)")
}

// Headroom-fix verification: with MaxRunners=10, 8 Running and 1 Pending real
// runner pods, only 1 placeholder pair may be created (not 10 — that was the
// pre-fix bug that allowed up to MaxRunners placeholders ON TOP OF the real
// runners, doubling the cap).
func TestReconcile_MaxRunnersHeadroom_CountsPendingTowardCap(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 50},
	}
	cfg := Config{
		ProactiveCapacity:  0,
		MaxRunners:         10,
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, hudRows)
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 8, corev1.PodRunning, "runner-r")
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 1, corev1.PodPending, "runner-p")

	m.reconcileProvisioning(context.Background())

	// MaxRunners-headroom = 10 - (8 Running + 1 Pending) = 1.
	// desired = min(50, 1) = 1 pair = 2 pods.
	pairs, err := m.placeholders.ListPairs(context.Background())
	require.NoError(t, err)
	assert.Len(t, pairs, 1,
		"only 1 placeholder allowed: MaxRunners=10 minus 8 Running minus 1 Pending real runner pods")
}

// runProvisioner must respect ctx.Done() while waiting out the startup
// jitter, so a listener shutting down during the jitter window exits
// promptly instead of blocking for up to RecalculateInterval.
func TestRunProvisioner_JitterRespectsContextCancellation(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   1,
		MaxRunners:          5,
		PlaceholderTimeout:  5 * time.Minute,
		RecalculateInterval: 1 * time.Hour, // Force a long jitter window
	}
	m, _, _ := newTestMonitor(t, cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.runProvisioner(ctx)
		close(done)
	}()

	// Give the goroutine a moment to enter the jitter sleep, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Good — exited promptly after cancel.
	case <-time.After(2 * time.Second):
		t.Fatal("runProvisioner did not exit within 2s after ctx cancel; jitter likely blocking")
	}
}

// The jitter window must be bounded by RecalculateInterval. With a tiny
// interval, the first reconcileProvisioning tick must fire quickly — this
// guards against an accidental jitter > interval misconfiguration that
// would stretch the first tick out to multiple intervals.
func TestRunProvisioner_JitterBoundedByInterval(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   1,
		MaxRunners:          5,
		PlaceholderTimeout:  5 * time.Minute,
		RecalculateInterval: 50 * time.Millisecond,
	}
	m, _, _ := newTestMonitor(t, cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go m.runProvisioner(ctx)

	// jitter is in [0, 50ms); first ticker fire is jitter + 50ms.
	// Allow generous wall-clock slack for CI noise but still tight enough
	// that an unbounded jitter (e.g. accidentally using the full ticker
	// duration twice) would fail.
	deadline := time.After(500 * time.Millisecond)
	for {
		pairs, err := m.placeholders.ListPairs(context.Background())
		require.NoError(t, err)
		if len(pairs) > 0 {
			return // First reconcile fired and created the proactive pair.
		}
		select {
		case <-deadline:
			t.Fatal("first provisioner tick did not fire within 500ms; jitter likely unbounded")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// pickJitter must return values strictly within [0, interval) and produce
// a non-degenerate distribution across many calls. Without spread, 50
// listeners would all land on the same delay and the desync goal fails.
func TestPickJitter_RandomizedAndInBounds(t *testing.T) {
	const interval = 100 * time.Millisecond
	const samples = 256

	buckets := make(map[time.Duration]struct{}, samples)
	for i := 0; i < samples; i++ {
		j := pickJitter(interval)
		assert.GreaterOrEqual(t, j, time.Duration(0),
			"jitter must be non-negative")
		assert.Less(t, j, interval,
			"jitter must be strictly less than interval")
		buckets[j.Round(time.Millisecond)] = struct{}{}
	}
	// Uniform over [0, 100ms) bucketed to 1ms should populate most buckets
	// in 256 samples; require well over a handful to catch a degenerate
	// "always zero" or "always interval/2" implementation.
	assert.Greater(t, len(buckets), 32,
		"jitter must spread across [0, interval); got %d unique 1ms buckets", len(buckets))
}

// Zero or negative interval is a misconfiguration; pickJitter must
// degrade to 0 rather than panic via rand.Int64N(0).
func TestPickJitter_NonPositiveInterval(t *testing.T) {
	assert.Equal(t, time.Duration(0), pickJitter(0))
	assert.Equal(t, time.Duration(0), pickJitter(-time.Second))
}

// time.NewTicker(0) panics, so a 0 RecalculateInterval misconfig must be
// handled before the ticker is created. runProvisioner should still do the
// initial reconcile and then park on ctx instead of crashing the listener.
func TestRunProvisioner_ZeroIntervalDoesNotPanic(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   1,
		MaxRunners:          5,
		PlaceholderTimeout:  5 * time.Minute,
		RecalculateInterval: 0,
	}
	m, _, _ := newTestMonitor(t, cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		m.runProvisioner(ctx)
		close(done)
	}()

	// Give the goroutine time to do the initial reconcile and park on ctx.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runProvisioner did not exit within 2s after ctx cancel")
	}
}

// Edge case: real runner pods have already exhausted (or exceeded) MaxRunners.
// The headroom subtraction goes negative -> max(0) clamps it -> desiredPairs=0,
// no placeholders are created. Prevents over-provisioning when the cap is hit.
func TestReconcile_MaxRunnersHeadroom_AtOrAboveCapFloorsAtZero(t *testing.T) {
	hudRows := []QueuedJobsForRunner{
		{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 50},
	}
	cfg := Config{
		ProactiveCapacity:  5,
		MaxRunners:         10,
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, hudRows)
	// 12 real runner pods: 6 Running + 6 Pending — already over the cap of 10.
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 6, corev1.PodRunning, "runner-r")
	createRealRunnerPods(t, cs, "test-ns", "test-sset", 6, corev1.PodPending, "runner-p")

	m.reconcileProvisioning(context.Background())

	// MaxRunners-headroom = 10 - 12 = -2; max(...,0) -> 0 pairs.
	pairs, err := m.placeholders.ListPairs(context.Background())
	require.NoError(t, err)
	assert.Empty(t, pairs,
		"total runner pods (12) >= MaxRunners (10) must floor desiredPairs to 0")
}

// newShardingTestMonitor wires the monitor to a HUD test server whose
// response varies by the queuedThresholdMinutes parameter:
//   - threshold == 0 returns nTotal rows
//   - threshold > 0 returns nAged rows
func newShardingTestMonitor(
	t *testing.T,
	cfg Config,
	label string,
	nTotal, nAged int,
) (*Monitor, *fake.Clientset, *atomic.Int32) {
	t.Helper()
	if cfg.Namespace == "" {
		cfg.Namespace = "test-ns"
	}
	if cfg.ScaleSetName == "" {
		cfg.ScaleSetName = "test-sset"
	}
	if cfg.HUDAPIToken == "" {
		cfg.HUDAPIToken = "test"
	}
	if cfg.FreshMultiplier == 0 {
		cfg.FreshMultiplier = 1.0
	}
	if cfg.AgedMultiplier == 0 {
		cfg.AgedMultiplier = 1.0
	}
	cs := newFakeClientset()

	var maxRunnersVal atomic.Int32
	setMax := func(v int) { maxRunnersVal.Store(int32(v)) }

	logger := discardLogger

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paramsRaw := r.URL.Query().Get("parameters")
		var got struct {
			QueuedThresholdMinutes int `json:"queuedThresholdMinutes"`
		}
		require.NoError(t, json.Unmarshal([]byte(paramsRaw), &got))
		count := nTotal
		if got.QueuedThresholdMinutes > 0 {
			count = nAged
		}
		w.WriteHeader(http.StatusOK)
		require.NoError(t, json.NewEncoder(w).Encode([]QueuedJobsForRunner{
			{RunnerLabel: label, NumQueuedJobs: count},
		}))
	}))
	t.Cleanup(srv.Close)

	m := &Monitor{
		config: cfg,
		placeholders: NewPlaceholderManager(
			cs, cfg.Namespace, "test-listener", cfg, logger,
		),
		hudClient:     NewHUDClient(srv.URL, "test"),
		clientset:     cs,
		setMaxRunners: setMax,
		logger:        logger.With("component", "capacity-monitor"),
		recorder:      metrics.DiscardCapacity,
	}
	return m, cs, &maxRunnersVal
}

// Sharding: even split with no remainder. nFresh=10, ClusterCount=2 →
// index 0 takes 5, no aged jobs → queuedJobs=5.
func TestReconcile_Sharding_EvenSplit_Index0(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        2,
		AgeThresholdSeconds: 900,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 0)
	m.reconcileProvisioning(context.Background())

	// nFresh=10, mySlice=10/2=5, 0 < 10%2=0? no -> 5; queued=5+0=5 -> 10 pods
	assert.Equal(t, 10, countPods(t, cs, "test-ns"))
}

// Sharding: remainder distribution. nFresh=11, ClusterCount=2 →
// index 0 gets the +1 (6), index 1 gets 5. This test pins index 1.
func TestReconcile_Sharding_RemainderToLowerIndex_Index1(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        1,
		ClusterCount:        2,
		AgeThresholdSeconds: 900,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 11, 0)
	m.reconcileProvisioning(context.Background())

	// nFresh=11, mySlice=11/2=5, 1 < 11%2=1? no -> 5; queued=5+0=5 -> 10 pods
	assert.Equal(t, 10, countPods(t, cs, "test-ns"))
}

// Sharding with aged jobs: aged are claimed in full by every cluster,
// fresh are sliced. Index 2 of 3, nTotal=10, nAged=3 → nFresh=7,
// mySlice=7/3=2 (index 2 doesn't get the remainder since 2 >= 7%3=1),
// queued = 2 + 3 = 5.
func TestReconcile_Sharding_WithAgedJobs(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        2,
		ClusterCount:        3,
		AgeThresholdSeconds: 900,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 3)
	m.reconcileProvisioning(context.Background())

	// nFresh=7, mySlice=7/3=2, 2 < 7%3=1? no -> 2; queued=2+3=5 -> 10 pods
	assert.Equal(t, 10, countPods(t, cs, "test-ns"))
}

// Sharding: nAged > nTotal would otherwise produce negative nFresh.
// max(0, nTotal-nAged) clamps it; aged are still claimed in full.
func TestReconcile_Sharding_AgedExceedsTotal_ClampsFresh(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        2,
		AgeThresholdSeconds: 900,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 15)
	m.reconcileProvisioning(context.Background())

	// nFresh=max(0,10-15)=0, mySlice=0, queued=0+15=15 -> 30 pods
	assert.Equal(t, 30, countPods(t, cs, "test-ns"))
}

// Sharding disabled: AgeThreshold=0 must fall through to the legacy
// single-query path even when ClusterCount > 1. The HUD server returns
// `nTotal` (threshold=0 reply) — but with sharding disabled the monitor
// must consume that count whole, ignoring the aged-query path entirely.
func TestReconcile_Sharding_DisabledByZeroAgeThreshold(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        5,
		AgeThresholdSeconds: 0,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 8, 99)
	m.reconcileProvisioning(context.Background())

	// Sharding disabled -> queued = nTotal = 8 -> 16 pods.
	// If sharding were active, queued would be 8/5+99 (way higher).
	assert.Equal(t, 16, countPods(t, cs, "test-ns"),
		"AgeThreshold=0 must short-circuit to the single-query (legacy) path")
}

// Sharding disabled: ClusterCount=1 must fall through to the legacy path
// even when AgeThreshold is set — a single-cluster deployment has nothing
// to shard. (1/1 = 1, but we still skip the second HUD call entirely.)
func TestReconcile_Sharding_DisabledBySingleCluster(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        1,
		AgeThresholdSeconds: 900,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 8, 99)
	m.reconcileProvisioning(context.Background())

	// Sharding disabled -> queued = nTotal = 8 -> 16 pods.
	assert.Equal(t, 16, countPods(t, cs, "test-ns"),
		"ClusterCount=1 must short-circuit to the single-query (legacy) path")
}

func TestReconcile_Sharding_FreshMultiplierHalf(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        2,
		AgeThresholdSeconds: 900,
		FreshMultiplier:     0.5,
		AgedMultiplier:      1.0,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 0)
	m.reconcileProvisioning(context.Background())

	assert.Equal(t, 6, countPods(t, cs, "test-ns"))
}

func TestReconcile_Sharding_FreshMultiplierDouble(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        2,
		AgeThresholdSeconds: 900,
		FreshMultiplier:     2.0,
		AgedMultiplier:      1.0,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 0)
	m.reconcileProvisioning(context.Background())

	assert.Equal(t, 20, countPods(t, cs, "test-ns"))
}

func TestReconcile_Sharding_AgedMultiplierHalf(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        2,
		AgeThresholdSeconds: 900,
		FreshMultiplier:     1.0,
		AgedMultiplier:      0.5,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 8)
	m.reconcileProvisioning(context.Background())

	assert.Equal(t, 10, countPods(t, cs, "test-ns"))
}

func TestReconcile_Sharding_AgedMultiplierDouble(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        2,
		AgeThresholdSeconds: 900,
		FreshMultiplier:     1.0,
		AgedMultiplier:      2.0,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 7)
	m.reconcileProvisioning(context.Background())

	assert.Equal(t, 32, countPods(t, cs, "test-ns"))
}

func TestReconcile_Sharding_BothMultipliersZero(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        2,
		AgeThresholdSeconds: 900,
		FreshMultiplier:     0.0001,
		AgedMultiplier:      0.0001,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 5)
	m.config.FreshMultiplier = 0
	m.config.AgedMultiplier = 0
	m.reconcileProvisioning(context.Background())

	assert.Equal(t, 0, countPods(t, cs, "test-ns"))
}

func TestReconcile_Unsharded_FreshMultiplier(t *testing.T) {
	cfg := Config{
		ProactiveCapacity:   0,
		MaxRunners:          100,
		ScaleSetLabels:      []string{"linux.2xlarge"},
		PlaceholderTimeout:  5 * time.Minute,
		ClusterIndex:        0,
		ClusterCount:        1,
		AgeThresholdSeconds: 0,
		FreshMultiplier:     0.5,
		AgedMultiplier:      2.0,
	}
	m, cs, _ := newShardingTestMonitor(t, cfg, "linux.2xlarge", 10, 99)
	m.reconcileProvisioning(context.Background())

	assert.Equal(t, 10, countPods(t, cs, "test-ns"))
}
