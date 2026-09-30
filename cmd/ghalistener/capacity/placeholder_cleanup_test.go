package capacity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestProvisioner_TerminalPairsAtBurstCap_Replaced checks that placeholder
// pairs where a pod Succeeded (its sleep ran out) or Failed are deleted and
// replaced while desired is pinned at the burst cap, so advertised capacity
// recovers, and that Running and Pending pairs are kept. Counted as current,
// the terminal pairs would hold current at desired and never be replaced.
func TestProvisioner_TerminalPairsAtBurstCap_Replaced(t *testing.T) {
	hudRows := []QueuedJobsForRunner{{RunnerLabel: "linux.2xlarge", NumQueuedJobs: 10}}
	cfg := Config{
		MaxRunners:         unlimitedMaxRunners,
		MaxBurstCapacity:   5,
		ScaleSetLabels:     []string{"linux.2xlarge"},
		PlaceholderTimeout: 5 * time.Minute,
	}
	m, cs, maxVal := newTestMonitor(t, cfg, hudRows)
	rec := newFakeCapacityRecorder()
	m.recorder = rec
	ctx := context.Background()

	m.reconcileProvisioning(ctx)
	terminal, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	require.Len(t, terminal, 5, "desired = min(10 queued, MaxBurstCapacity 5)")
	i := 0
	for slotID, pair := range terminal {
		i++
		if i == 1 {
			setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodSucceeded)
			continue
		}
		setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodRunning)
		name, phase := pair.WorkflowPod.Name, corev1.PodSucceeded
		if i == 2 {
			name, phase = pair.RunnerPod.Name, corev1.PodFailed
		}
		pod, err := cs.CoreV1().Pods("test-ns").Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		pod.Status.Phase = phase
		_, err = cs.CoreV1().Pods("test-ns").UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	// Fresh creation timestamps keep the Pending pair from timing out; the
	// fake clientset leaves them at zero.
	for slotID, phase := range map[string]corev1.PodPhase{"running": corev1.PodRunning, "pending": corev1.PodPending} {
		require.NoError(t, m.placeholders.CreatePair(ctx, slotID))
		setPodsCreationAndPhase(t, cs, ctx, "test-ns", slotID, time.Now(), phase)
	}

	m.reconcileProvisioning(ctx)

	after, err := m.placeholders.ListPairs(ctx)
	require.NoError(t, err)
	assert.Len(t, after, 5, "terminal pairs replaced up to desired")
	assert.Contains(t, after, "running")
	assert.Contains(t, after, "pending")
	for slotID := range after {
		if _, ok := terminal[slotID]; ok {
			t.Errorf("terminal pair %s was not deleted", slotID)
			continue
		}
		setPodsPhase(t, cs, ctx, "test-ns", slotID, corev1.PodRunning)
	}
	m.reconcileReporting(ctx)
	assert.Equal(t, int32(5), maxVal.Load(), "advertised capacity back at the burst cap")

	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Equal(t, map[string]int{deleteReasonTerminal + ":" + resultSuccess: 5}, rec.incPairDeletesCalls)
}
