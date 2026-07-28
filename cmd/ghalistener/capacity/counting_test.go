package capacity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testScaleSet   = "sset"
	testListenerID = "L1"
)

func mkRealRunner(name string, phase corev1.PodPhase, node string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				labelEphemeralRunner: labelEphemeralRunnerValue,
				labelScaleSet:        testScaleSet,
			},
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// mkRealWorkflow builds the single job pod the hook creates per runner: the
// name carries the "-workflow" suffix and the pod is labelled
// runner-pod=<runnerPodName>. The counter selects the workflow slot off this
// suffix.
func mkRealWorkflow(name, runnerPodName string, phase corev1.PodPhase, node string) corev1.Pod {
	return mkRunnerPodChild(name+workflowPodNameSuffix, runnerPodName, phase, node)
}

// mkRealStepPod builds a container-step / Docker-action step pod: it carries
// the SAME runner-pod label as the job pod but a "-step-<hex>" name (never the
// "-workflow" suffix), so the counter must not credit it as its own slot.
func mkRealStepPod(name, runnerPodName string, phase corev1.PodPhase, node string) corev1.Pod {
	return mkRunnerPodChild(name+"-step-abc12345", runnerPodName, phase, node)
}

func mkRunnerPodChild(podName, runnerPodName string, phase corev1.PodPhase, node string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   podName,
			Labels: map[string]string{labelWorkflowRunnerPod: runnerPodName},
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func mkPlaceholder(name, role, slotID string, phase corev1.PodPhase, node string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				labelManagedBy:       managedByValue,
				labelScaleSet:        testScaleSet,
				labelPlaceholderID:   slotID,
				labelPlaceholderRole: role,
				labelListenerPod:     testListenerID,
			},
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func terminating(pod corev1.Pod) corev1.Pod {
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	return pod
}

// TestClassifyCapacityPods covers the core counter: capacity is the scarcer
// of the fungible runner/workflow sides, each side summing real + placeholder
// halves. Core mode (nil schedulability check).
func TestClassifyCapacityPods(t *testing.T) {
	tests := []struct {
		name string
		pods []corev1.Pod
		want capacityCounts
	}{
		{
			name: "empty",
			pods: nil,
			want: capacityCounts{},
		},
		{
			// Case 1: placeholders only, balanced.
			name: "all placeholders balanced",
			pods: []corev1.Pod{
				mkPlaceholder("phr1", rolePlaceholderRunner, "s1", corev1.PodRunning, "n1"),
				mkPlaceholder("phw1", rolePlaceholderWorkflow, "s1", corev1.PodRunning, "n1"),
				mkPlaceholder("phr2", rolePlaceholderRunner, "s2", corev1.PodRunning, "n2"),
				mkPlaceholder("phw2", rolePlaceholderWorkflow, "s2", corev1.PodRunning, "n2"),
			},
			want: capacityCounts{placeholderRunners: 2, placeholderWorkflows: 2, runningPairs: 2},
		},
		{
			// Case 2: real complete pairs only.
			name: "all real complete pairs",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodRunning, "n1"),
				mkRealRunner("r2", corev1.PodRunning, "n2"),
				mkRealWorkflow("w1", "r1", corev1.PodRunning, "n1"),
				mkRealWorkflow("w2", "r2", corev1.PodRunning, "n2"),
			},
			want: capacityCounts{realRunners: 2, realWorkflows: 2},
		},
		{
			// Case 3 (PRIMARY bug): real runners starved of workflow slots.
			// Three runners, one workflow -> capacity 1, not 3.
			name: "workflow-constrained",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodRunning, "n1"),
				mkRealRunner("r2", corev1.PodRunning, "n2"),
				mkRealRunner("r3", corev1.PodRunning, "n3"),
				mkRealWorkflow("w1", "r1", corev1.PodRunning, "n1"),
			},
			want: capacityCounts{realRunners: 3, realWorkflows: 1},
		},
		{
			// Case 4: runner-constrained. A single runner runs a container-step
			// workflow, so the hook creates one job pod plus step pods, all
			// labelled runner-pod=r1. The workflow side must credit r1 once —
			// only the "-workflow"-suffixed job pod counts, not the step pods.
			name: "runner-constrained",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodRunning, "n1"),
				mkRealWorkflow("w1", "r1", corev1.PodRunning, "n1"),
				mkRealStepPod("s1", "r1", corev1.PodRunning, "n1"),
				mkRealStepPod("s2", "r1", corev1.PodRunning, "n1"),
			},
			want: capacityCounts{realRunners: 1, realWorkflows: 1},
		},
		{
			// Case 5: real + placeholder on the same side are fungible.
			name: "mixed real and placeholder halves",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodRunning, "n1"),
				mkPlaceholder("phr1", rolePlaceholderRunner, "s1", corev1.PodRunning, "n2"),
				mkPlaceholder("phw1", rolePlaceholderWorkflow, "s1", corev1.PodRunning, "n2"),
				mkPlaceholder("phw2", rolePlaceholderWorkflow, "s2", corev1.PodRunning, "n3"),
			},
			want: capacityCounts{
				realRunners:        1,
				placeholderRunners: 1, placeholderWorkflows: 2,
				runningPairs: 1, // slot s1 has both halves Running
			},
		},
		{
			// Case 6 (fungibility): crossed preemption leaves orphan halves from
			// DIFFERENT slots. Summing each side then taking the min still yields
			// 1 unit of capacity: one runner half and one workflow half exist.
			name: "crossed preemption orphan halves",
			pods: []corev1.Pod{
				mkPlaceholder("phrA", rolePlaceholderRunner, "A", corev1.PodRunning, "nA"),
				mkPlaceholder("phwB", rolePlaceholderWorkflow, "B", corev1.PodRunning, "nB"),
			},
			want: capacityCounts{placeholderRunners: 1, placeholderWorkflows: 1},
		},
		{
			// Case 7: nothing schedulable/committed.
			name: "only non-counting pods",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodPending, ""),                                 // unscheduled
				mkPlaceholder("phr1", rolePlaceholderRunner, "s1", corev1.PodPending, ""), // pending placeholder
			},
			want: capacityCounts{},
		},
		{
			name: "terminal-phase real pods excluded",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodSucceeded, "n1"),
				mkRealRunner("r2", corev1.PodFailed, "n2"),
				mkRealRunner("r3", corev1.PodRunning, "n3"),
				mkRealWorkflow("w1", "r3", corev1.PodSucceeded, "n3"),
				mkRealWorkflow("w2", "r3", corev1.PodRunning, "n3"),
			},
			// r1/r2 terminal -> not bound -> excluded. Only r3 is counted; its
			// finished job pod (w1) is dropped by terminal phase, its live job
			// pod (w2) counts once.
			want: capacityCounts{realRunners: 1, realWorkflows: 1},
		},
		{
			name: "terminating real and placeholder excluded",
			pods: []corev1.Pod{
				terminating(mkRealRunner("r1", corev1.PodRunning, "n1")),
				mkRealRunner("r2", corev1.PodRunning, "n2"),
				mkRealWorkflow("w2", "r2", corev1.PodRunning, "n2"),
				terminating(mkPlaceholder("phr1", rolePlaceholderRunner, "s1", corev1.PodRunning, "n3")),
				mkPlaceholder("phw1", rolePlaceholderWorkflow, "s1", corev1.PodRunning, "n3"),
			},
			// r1 terminating -> not bound -> excluded from realRunners (so its
			// workflow could not anchor either). phr1 terminating -> excluded from
			// placeholderRunners, but the phase-based runningPairs gauge still sees
			// slot s1's two Running halves.
			want: capacityCounts{realRunners: 1, realWorkflows: 1, placeholderWorkflows: 1, runningPairs: 1},
		},
		{
			name: "bound pending real pod counts",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodPending, "n1"), // ContainerCreating with node
				mkRealWorkflow("w1", "r1", corev1.PodPending, "n1"),
			},
			want: capacityCounts{realRunners: 1, realWorkflows: 1},
		},
		{
			name: "foreign scale set and listener ignored",
			pods: []corev1.Pod{
				// Real workflow whose runner-pod points at a runner NOT in this
				// scale set -> ignored (runner name unknown).
				mkRealWorkflow("w-foreign", "r-other-sset", corev1.PodRunning, "n1"),
				// Placeholder from a different listener -> ignored.
				func() corev1.Pod {
					p := mkPlaceholder("phr-other", rolePlaceholderRunner, "s9", corev1.PodRunning, "n1")
					p.Labels[labelListenerPod] = "other-listener"
					return p
				}(),
				// Placeholder from a different scale set -> ignored.
				func() corev1.Pod {
					p := mkPlaceholder("phw-other", rolePlaceholderWorkflow, "s9", corev1.PodRunning, "n1")
					p.Labels[labelScaleSet] = "other-sset"
					return p
				}(),
			},
			want: capacityCounts{},
		},
		{
			// Real runners bound and Running but no workflow pods at all: the
			// workflow side is empty so capacity is 0 even though runners exist.
			name: "real runners running with zero workflows -> capacity 0",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodRunning, "n1"),
				mkRealRunner("r2", corev1.PodRunning, "n2"),
			},
			want: capacityCounts{realRunners: 2},
		},
		{
			// Unknown-phase pods are terminal for counting purposes: an Unknown
			// runner is not counted (nor anchors its workflow), and an Unknown
			// job pod is not counted.
			name: "unknown phase pods excluded",
			pods: []corev1.Pod{
				mkRealRunner("r1", corev1.PodUnknown, "n1"),
				mkRealRunner("r2", corev1.PodRunning, "n2"),
				mkRealWorkflow("w1", "r1", corev1.PodRunning, "n1"),
				mkRealWorkflow("w2", "r2", corev1.PodUnknown, "n2"),
				mkRealWorkflow("w3", "r2", corev1.PodRunning, "n2"),
			},
			want: capacityCounts{realRunners: 1, realWorkflows: 1},
		},
		{
			// Regression (FIX 1): runner A runs a container-step workflow (job
			// pod + a same-runner-pod step pod, both bound) while runner B is
			// Running with its workflow still unscheduled. The step pod must not
			// add a second workflow slot: capacity = min(2, 1) = 1, not 2.
			name: "container-step pod does not double-count workflow slot",
			pods: []corev1.Pod{
				mkRealRunner("rA", corev1.PodRunning, "nA"),
				mkRealWorkflow("wA", "rA", corev1.PodRunning, "nA"),
				mkRealStepPod("sA", "rA", corev1.PodRunning, "nA"),
				mkRealRunner("rB", corev1.PodRunning, "nB"),
			},
			want: capacityCounts{realRunners: 2, realWorkflows: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyCapacityPods(tt.pods, testScaleSet, testListenerID, nil)
			assert.Equal(t, tt.want, got, "counts")
			assert.Equal(t, min(tt.want.runnerSide(), tt.want.workflowSide()), got.capacity(), "capacity")
		})
	}
}

// TestClassifyCapacityPods_Capacity spot-checks capacity() for the sides.
func TestClassifyCapacityPods_Capacity(t *testing.T) {
	pods := []corev1.Pod{
		mkRealRunner("r1", corev1.PodRunning, "n1"),
		mkRealRunner("r2", corev1.PodRunning, "n2"),
		mkRealRunner("r3", corev1.PodRunning, "n3"),
		mkRealWorkflow("w1", "r1", corev1.PodRunning, "n1"),
	}
	got := classifyCapacityPods(pods, testScaleSet, testListenerID, nil)
	assert.Equal(t, 3, got.runnerSide())
	assert.Equal(t, 1, got.workflowSide())
	assert.Equal(t, 1, got.capacity(), "min of the two sides")
}
