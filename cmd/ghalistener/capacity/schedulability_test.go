package capacity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func taint(key string, effect corev1.TaintEffect) corev1.Taint {
	return corev1.Taint{Key: key, Effect: effect}
}

func node(name string, unschedulable bool, taints ...corev1.Taint) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{Unschedulable: unschedulable, Taints: taints},
	}
}

func podTolerating(tolerations ...corev1.Toleration) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{Tolerations: tolerations}}
}

func TestNodeSchedulableForReplacement(t *testing.T) {
	instanceTypeTol := corev1.Toleration{Key: "instance-type", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}

	tests := []struct {
		name string
		node *corev1.Node
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "no taints, not cordoned",
			node: node("n1", false),
			pod:  podTolerating(),
			want: true,
		},
		{
			name: "cordoned excludes even with no taints",
			node: node("n1", true),
			pod:  podTolerating(),
			want: false,
		},
		{
			name: "untolerated NoSchedule taint",
			node: node("n1", false, taint("karpenter.sh/disrupted", corev1.TaintEffectNoSchedule)),
			pod:  podTolerating(),
			want: false,
		},
		{
			name: "untolerated NoExecute taint",
			node: node("n1", false, taint("node.kubernetes.io/not-ready", corev1.TaintEffectNoExecute)),
			pod:  podTolerating(),
			want: false,
		},
		{
			name: "tolerated NoSchedule taint",
			node: node("n1", false, taint("instance-type", corev1.TaintEffectNoSchedule)),
			pod:  podTolerating(instanceTypeTol),
			want: true,
		},
		{
			name: "tolerated NoExecute taint",
			node: node("n1", false, taint("instance-type", corev1.TaintEffectNoExecute)),
			pod: podTolerating(corev1.Toleration{
				Key: "instance-type", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute,
			}),
			want: true,
		},
		{
			name: "PreferNoSchedule taint is ignored",
			node: node("n1", false, taint("whatever", corev1.TaintEffectPreferNoSchedule)),
			pod:  podTolerating(),
			want: true,
		},
		{
			name: "one untolerated among several excludes",
			node: node("n1", false,
				taint("instance-type", corev1.TaintEffectNoSchedule),
				taint("karpenter.sh/disrupted", corev1.TaintEffectNoSchedule),
			),
			pod:  podTolerating(instanceTypeTol),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, nodeSchedulableForReplacement(tt.node, tt.pod))
		})
	}
}

func TestTolerationToleratesTaint(t *testing.T) {
	tests := []struct {
		name       string
		toleration corev1.Toleration
		taint      corev1.Taint
		want       bool
	}{
		{
			name:       "Exists matches any value",
			toleration: corev1.Toleration{Key: "k", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
			taint:      corev1.Taint{Key: "k", Value: "anything", Effect: corev1.TaintEffectNoSchedule},
			want:       true,
		},
		{
			name:       "Equal matches equal value",
			toleration: corev1.Toleration{Key: "node-fleet", Operator: corev1.TolerationOpEqual, Value: "g4dn", Effect: corev1.TaintEffectNoSchedule},
			taint:      corev1.Taint{Key: "node-fleet", Value: "g4dn", Effect: corev1.TaintEffectNoSchedule},
			want:       true,
		},
		{
			name:       "Equal rejects different value",
			toleration: corev1.Toleration{Key: "node-fleet", Operator: corev1.TolerationOpEqual, Value: "g4dn", Effect: corev1.TaintEffectNoSchedule},
			taint:      corev1.Taint{Key: "node-fleet", Value: "c7a", Effect: corev1.TaintEffectNoSchedule},
			want:       false,
		},
		{
			name:       "empty operator defaults to Equal",
			toleration: corev1.Toleration{Key: "k", Value: "v", Effect: corev1.TaintEffectNoSchedule},
			taint:      corev1.Taint{Key: "k", Value: "v", Effect: corev1.TaintEffectNoSchedule},
			want:       true,
		},
		{
			name:       "empty effect matches all effects",
			toleration: corev1.Toleration{Key: "k", Operator: corev1.TolerationOpExists},
			taint:      corev1.Taint{Key: "k", Effect: corev1.TaintEffectNoExecute},
			want:       true,
		},
		{
			name:       "specific effect mismatch",
			toleration: corev1.Toleration{Key: "k", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
			taint:      corev1.Taint{Key: "k", Effect: corev1.TaintEffectNoExecute},
			want:       false,
		},
		{
			name:       "key mismatch",
			toleration: corev1.Toleration{Key: "a", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
			taint:      corev1.Taint{Key: "b", Effect: corev1.TaintEffectNoSchedule},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tolerationToleratesTaint(&tt.toleration, &tt.taint))
		})
	}
}

// TestClassifyCapacityPods_SchedulabilityExcludes drives the classifier with a
// schedulability func to confirm exclusions land in unschedulablePlaceholders
// and do not count toward capacity.
func TestClassifyCapacityPods_SchedulabilityExcludes(t *testing.T) {
	pods := []corev1.Pod{
		mkPlaceholder("phr-good", rolePlaceholderRunner, "g", corev1.PodRunning, "n-good"),
		mkPlaceholder("phw-good", rolePlaceholderWorkflow, "g", corev1.PodRunning, "n-good"),
		mkPlaceholder("phr-bad", rolePlaceholderRunner, "b", corev1.PodRunning, "n-bad"),
		mkPlaceholder("phw-bad", rolePlaceholderWorkflow, "b", corev1.PodRunning, "n-bad"),
	}
	// Only pods on n-good are schedulable.
	schedulable := func(pod *corev1.Pod) bool { return pod.Spec.NodeName == "n-good" }

	got := classifyCapacityPods(pods, testScaleSet, testListenerID, schedulable)

	assert.Equal(t, 1, got.placeholderRunners)
	assert.Equal(t, 1, got.placeholderWorkflows)
	assert.Equal(t, 2, got.unschedulablePlaceholders)
	assert.Equal(t, 1, got.capacity())
	// runningPairs is phase-based and independent of schedulability.
	assert.Equal(t, 2, got.runningPairs)
}

// TestClassifyCapacityPods_MissingNodeExcludes confirms the fail-safe: a
// checker that reports a placeholder's node as gone drops it.
func TestClassifyCapacityPods_MissingNodeExcludes(t *testing.T) {
	pods := []corev1.Pod{
		mkPlaceholder("phr", rolePlaceholderRunner, "s", corev1.PodRunning, "ghost-node"),
		mkPlaceholder("phw", rolePlaceholderWorkflow, "s", corev1.PodRunning, "ghost-node"),
	}
	got := classifyCapacityPods(pods, testScaleSet, testListenerID, func(*corev1.Pod) bool { return false })
	assert.Equal(t, 0, got.capacity())
	assert.Equal(t, 2, got.unschedulablePlaceholders)
}

// TestClassifyCapacityPods_DegradeToCore confirms nil checker counts every
// Running placeholder (no schedulability refinement, no exclusions).
func TestClassifyCapacityPods_DegradeToCore(t *testing.T) {
	pods := []corev1.Pod{
		mkPlaceholder("phr", rolePlaceholderRunner, "s", corev1.PodRunning, "n-cordoned"),
		mkPlaceholder("phw", rolePlaceholderWorkflow, "s", corev1.PodRunning, "n-cordoned"),
	}
	got := classifyCapacityPods(pods, testScaleSet, testListenerID, nil)
	assert.Equal(t, 1, got.capacity())
	assert.Equal(t, 0, got.unschedulablePlaceholders)
}

// TestReporter_SchedulabilityCheck exercises the full reporter path with a
// live node informer: a placeholder on a cordoned node must drop out of the
// advertised capacity.
func TestReporter_SchedulabilityCheck(t *testing.T) {
	cfg := Config{
		MaxRunners:                    unlimitedMaxRunners,
		EnableNodeSchedulabilityCheck: true,
		PlaceholderTimeout:            5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	// Align the monitor's identity with the labels mkPlaceholder stamps.
	m.config.ScaleSetName = testScaleSet
	m.placeholders.listenerID = testListenerID
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := cs.CoreV1().Nodes().Create(ctx, node("n-good", false), metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = cs.CoreV1().Nodes().Create(ctx, node("n-cordoned", true), metav1.CreateOptions{})
	require.NoError(t, err)

	// Slot g on the schedulable node; slot c on the cordoned node.
	for _, p := range []corev1.Pod{
		mkPlaceholder("phr-g", rolePlaceholderRunner, "g", corev1.PodRunning, "n-good"),
		mkPlaceholder("phw-g", rolePlaceholderWorkflow, "g", corev1.PodRunning, "n-good"),
		mkPlaceholder("phr-c", rolePlaceholderRunner, "c", corev1.PodRunning, "n-cordoned"),
		mkPlaceholder("phw-c", rolePlaceholderWorkflow, "c", corev1.PodRunning, "n-cordoned"),
	} {
		p.Namespace = "test-ns"
		_, err := cs.CoreV1().Pods("test-ns").Create(ctx, &p, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	m.nodeWatcher = newNodeSchedulabilityWatcher(cs, true)
	m.nodeWatcher.start(ctx, discardLogger)
	require.True(t, cache.WaitForCacheSync(ctx.Done(), m.nodeWatcher.hasSynced),
		"node cache should sync from fake clientset")

	m.reconcileReporting(ctx)

	// Only the n-good slot is a valid reserve: min(1, 1) = 1.
	assert.Equal(t, int32(1), maxVal.Load())
}

// TestReporter_SchedulabilityDegradesToCore confirms that when the check is
// enabled but the node cache never synced, the reporter counts every Running
// placeholder (core mode) and records a skip.
func TestReporter_SchedulabilityDegradesToCore(t *testing.T) {
	cfg := Config{
		MaxRunners:                    unlimitedMaxRunners,
		EnableNodeSchedulabilityCheck: true,
		PlaceholderTimeout:            5 * time.Minute,
	}
	rec := newFakeCapacityRecorder()
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	m.recorder = rec
	m.config.ScaleSetName = testScaleSet
	m.placeholders.listenerID = testListenerID
	ctx := context.Background()

	// Watcher exists but was never started -> cache not synced -> degrade.
	m.nodeWatcher = newNodeSchedulabilityWatcher(cs, true)

	for _, p := range []corev1.Pod{
		mkPlaceholder("phr-c", rolePlaceholderRunner, "c", corev1.PodRunning, "n-cordoned"),
		mkPlaceholder("phw-c", rolePlaceholderWorkflow, "c", corev1.PodRunning, "n-cordoned"),
	} {
		p.Namespace = "test-ns"
		_, err := cs.CoreV1().Pods("test-ns").Create(ctx, &p, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	m.reconcileReporting(ctx)

	assert.Equal(t, int32(1), maxVal.Load(), "degrade-to-core counts the placeholder pair")
	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Equal(t, 1, rec.incReconcileSkipsCalls[skipReasonReporterNodeCacheUnsynced])
}

// TestReporter_SchedulabilitySelfHealsAfterLateSync proves the check is not
// latched off: a first cycle before the node cache syncs degrades to core mode
// (counts a cordoned-node placeholder), then a later cycle after the cache
// syncs applies the schedulability check and drops it.
func TestReporter_SchedulabilitySelfHealsAfterLateSync(t *testing.T) {
	cfg := Config{
		MaxRunners:                    unlimitedMaxRunners,
		EnableNodeSchedulabilityCheck: true,
		PlaceholderTimeout:            5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	m.config.ScaleSetName = testScaleSet
	m.placeholders.listenerID = testListenerID
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := cs.CoreV1().Nodes().Create(ctx, node("n-cordoned", true), metav1.CreateOptions{})
	require.NoError(t, err)
	for _, p := range []corev1.Pod{
		mkPlaceholder("phr-c", rolePlaceholderRunner, "c", corev1.PodRunning, "n-cordoned"),
		mkPlaceholder("phw-c", rolePlaceholderWorkflow, "c", corev1.PodRunning, "n-cordoned"),
	} {
		p.Namespace = "test-ns"
		_, err := cs.CoreV1().Pods("test-ns").Create(ctx, &p, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	// Watcher constructed but NOT started: first cycle is degraded, so the
	// cordoned-node pair is counted (fail-open).
	m.nodeWatcher = newNodeSchedulabilityWatcher(cs, true)
	m.reconcileReporting(ctx)
	assert.Equal(t, int32(1), maxVal.Load(), "degraded first cycle counts the pair despite the cordon")

	// Start the informer and let the cache sync late.
	m.nodeWatcher.start(ctx, discardLogger)
	require.True(t, cache.WaitForCacheSync(ctx.Done(), m.nodeWatcher.hasSynced))

	// The next cycle applies the schedulability check: the cordoned placeholder
	// is excluded and capacity self-heals to 0.
	m.reconcileReporting(ctx)
	assert.Equal(t, int32(0), maxVal.Load(),
		"self-heal: synced cache applies the check, cordoned placeholder excluded")
}

// TestPlaceholderSchedulabilityChecker_MissingNode drives the real checker
// (not a stub) with a synced cache to confirm the fail-safe branch: a
// placeholder whose node is absent from the cache — or that has no node — is
// excluded.
func TestPlaceholderSchedulabilityChecker_MissingNode(t *testing.T) {
	cfg := Config{
		MaxRunners:                    unlimitedMaxRunners,
		EnableNodeSchedulabilityCheck: true,
		PlaceholderTimeout:            5 * time.Minute,
	}
	m, cs, _ := newTestMonitor(t, cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No nodes exist in the cluster, but the cache still syncs (empty list).
	m.nodeWatcher = newNodeSchedulabilityWatcher(cs, true)
	m.nodeWatcher.start(ctx, discardLogger)
	require.True(t, cache.WaitForCacheSync(ctx.Done(), m.nodeWatcher.hasSynced))

	check := m.placeholderSchedulabilityChecker()
	require.NotNil(t, check, "checker must be active once the cache syncs")

	onGhostNode := mkPlaceholder("phr", rolePlaceholderRunner, "s", corev1.PodRunning, "ghost-node")
	assert.False(t, check(&onGhostNode), "node absent from cache -> excluded (fail safe)")

	noNode := mkPlaceholder("phr2", rolePlaceholderRunner, "s2", corev1.PodRunning, "")
	assert.False(t, check(&noNode), "placeholder with no node -> excluded")
}

// TestNodeSchedulableForReplacement_DeadNodeNotReadyBothTaints covers the
// realistic dead-node case: the node controller taints a NotReady node with
// not-ready under BOTH NoExecute and NoSchedule effects. A pod carries only the
// default not-ready:NoExecute toleration (tolerationSeconds 300) Kubernetes
// injects, so it tolerates the NoExecute taint but NOT the NoSchedule one ->
// the replacement could not schedule -> excluded.
func TestNodeSchedulableForReplacement_DeadNodeNotReadyBothTaints(t *testing.T) {
	deadNode := node("dead", false,
		taint("node.kubernetes.io/not-ready", corev1.TaintEffectNoSchedule),
		taint("node.kubernetes.io/not-ready", corev1.TaintEffectNoExecute),
	)
	ts := int64(300)
	pod := podTolerating(corev1.Toleration{
		Key:               "node.kubernetes.io/not-ready",
		Operator:          corev1.TolerationOpExists,
		Effect:            corev1.TaintEffectNoExecute,
		TolerationSeconds: &ts,
	})
	assert.False(t, nodeSchedulableForReplacement(deadNode, pod),
		"untolerated not-ready:NoSchedule taint on a dead node excludes the placeholder")
}

// TestReporter_UnschedulablePlaceholdersGauge_NonZero drives the full reporter
// path with a live informer and a cordoned node, asserting the
// unschedulable-placeholders gauge counts both dropped halves and that the
// reserve advertises no capacity.
func TestReporter_UnschedulablePlaceholdersGauge_NonZero(t *testing.T) {
	cfg := Config{
		MaxRunners:                    unlimitedMaxRunners,
		EnableNodeSchedulabilityCheck: true,
		PlaceholderTimeout:            5 * time.Minute,
	}
	rec := newFakeCapacityRecorder()
	m, cs, maxVal := newTestMonitor(t, cfg, nil)
	m.recorder = rec
	m.config.ScaleSetName = testScaleSet
	m.placeholders.listenerID = testListenerID
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := cs.CoreV1().Nodes().Create(ctx, node("n-cordoned", true), metav1.CreateOptions{})
	require.NoError(t, err)
	for _, p := range []corev1.Pod{
		mkPlaceholder("phr-c", rolePlaceholderRunner, "c", corev1.PodRunning, "n-cordoned"),
		mkPlaceholder("phw-c", rolePlaceholderWorkflow, "c", corev1.PodRunning, "n-cordoned"),
	} {
		p.Namespace = "test-ns"
		_, err := cs.CoreV1().Pods("test-ns").Create(ctx, &p, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	m.nodeWatcher = newNodeSchedulabilityWatcher(cs, true)
	m.nodeWatcher.start(ctx, discardLogger)
	require.True(t, cache.WaitForCacheSync(ctx.Done(), m.nodeWatcher.hasSynced))

	m.reconcileReporting(ctx)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Equal(t, 2, rec.unschedulablePlaceholders, "both halves on the cordoned node are dropped")
	assert.Equal(t, int32(0), maxVal.Load(), "cordoned-node reserve advertises no capacity")
}
