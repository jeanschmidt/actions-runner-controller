package capacity

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// labelEphemeralRunner / labelEphemeralRunnerValue mark a real
	// EphemeralRunner pod; combined with labelScaleSet they identify the
	// real runner pods of one scale set.
	labelEphemeralRunner      = "actions-ephemeral-runner"
	labelEphemeralRunnerValue = "True"

	// labelWorkflowRunnerPod is set by the k8s container hook (its
	// RunnerInstanceLabel) to the name of the runner pod that launched the
	// pod. The hook stamps it on the job pod AND on every container-step /
	// Docker-action step pod, so this label alone maps many pods to one
	// runner. Real workflow pods carry no scale-set label, so this is the
	// only link back to their scale set.
	labelWorkflowRunnerPod = "runner-pod"

	// workflowPodNameSuffix is appended by the hook's getJobPodName
	// (truncate(runner,54)+"-workflow") to name the single job pod per
	// runner. Step pods are named "<runner>-step-<hex>" and never carry this
	// suffix, so it selects exactly the job pod. The suffix is appended after
	// the runner-name truncation, so matching on it is truncation-safe.
	workflowPodNameSuffix = "-workflow"
)

// placeholderSchedulabilityFunc gates a Running, non-terminating placeholder
// pod on whether its node can still host the replacement real pod. A nil
// value means core mode: schedulability is not evaluated and every Running,
// non-terminating placeholder counts. The node-aware implementation lives in
// schedulability.go.
type placeholderSchedulabilityFunc func(pod *corev1.Pod) bool

// capacityCounts holds the fungible half-counts (plus observability extras)
// derived from a single namespace pod snapshot. A runner half and a workflow
// half are each satisfiable by either a real pod or a placeholder pod, so the
// advertisable capacity is the scarcer of the two sides.
type capacityCounts struct {
	realRunners          int
	realWorkflows        int
	placeholderRunners   int
	placeholderWorkflows int

	// runningPairs feeds the running-pairs gauge: slots whose runner AND
	// workflow placeholder halves are both Running.
	runningPairs int
	// unschedulablePlaceholders counts Running, non-terminating placeholders
	// dropped because their node can no longer host a replacement real pod.
	unschedulablePlaceholders int
}

func (c capacityCounts) runnerSide() int   { return c.realRunners + c.placeholderRunners }
func (c capacityCounts) workflowSide() int { return c.realWorkflows + c.placeholderWorkflows }

// capacity is the value advertised to GitHub (before the MaxRunners clamp):
// neither a runner without a schedulable workflow slot nor a workflow slot
// without a runner can execute a job, so the scarcer side bounds throughput.
func (c capacityCounts) capacity() int { return min(c.runnerSide(), c.workflowSide()) }

// starvedRunners is real runners counted on the runner side that lack a
// secured workflow slot. Clamped at zero so a bound runner whose workflow
// half is still pending (normal ContainerCreating) never reads negative.
func (c capacityCounts) starvedRunners() int { return max(0, c.realRunners-c.realWorkflows) }

// boundNonTerminal is the predicate for counting a real (main) runner or
// workflow pod: committed to a node, not mid-deletion, and not in a terminal
// phase. Terminal-phase exclusion drops Succeeded/Failed/Unknown pods so a
// finished runner (or a leftover job pod that has itself gone terminal) is
// not advertised as a live slot; requiring a node kills the image-pull dip
// (ContainerCreating is Pending-with-node) that a phase==Running gate suffers.
func boundNonTerminal(pod *corev1.Pod) bool {
	return pod.Spec.NodeName != "" &&
		pod.DeletionTimestamp == nil &&
		(pod.Status.Phase == corev1.PodPending || pod.Status.Phase == corev1.PodRunning)
}

// runningNotTerminating is the phase/lifecycle gate every placeholder must
// pass before the (optional) node-schedulability check. DeletionTimestamp==nil
// closes the preemption handoff window so a placeholder being torn down for
// its replacement is not double-counted alongside the real pod landing.
func runningNotTerminating(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil
}

// classifyCapacityPods derives capacity counts from one namespace pod
// snapshot. Taking a single atomic List (rather than one List per category)
// avoids cross-List races where a pod caught mid-preemption is counted on
// both the victim and preemptor sides.
//
// listenerID scopes placeholders to this listener so a peer listener's
// placeholders are never counted. schedulable gates placeholders on node
// schedulability (nil => core mode).
func classifyCapacityPods(
	pods []corev1.Pod,
	scaleSetName, listenerID string,
	schedulable placeholderSchedulabilityFunc,
) capacityCounts {
	var c capacityCounts

	// First pass: real runners and this listener's placeholders. Only a
	// bound-non-terminal runner is counted, and only such a runner anchors
	// the workflow-side match — a workflow whose runner is not itself counted
	// must not add a workflow slot.
	countedRunnerNames := make(map[string]struct{})
	var listenerPlaceholders []corev1.Pod
	for i := range pods {
		pod := &pods[i]
		l := pod.Labels
		switch {
		case l[labelEphemeralRunner] == labelEphemeralRunnerValue && l[labelScaleSet] == scaleSetName:
			if boundNonTerminal(pod) {
				c.realRunners++
				countedRunnerNames[pod.Name] = struct{}{}
			}
		case l[labelScaleSet] == scaleSetName && l[labelListenerPod] == listenerID &&
			(l[labelPlaceholderRole] == rolePlaceholderRunner || l[labelPlaceholderRole] == rolePlaceholderWorkflow):
			listenerPlaceholders = append(listenerPlaceholders, *pod)
			if !runningNotTerminating(pod) {
				continue
			}
			if schedulable != nil && !schedulable(pod) {
				c.unschedulablePlaceholders++
				continue
			}
			if l[labelPlaceholderRole] == rolePlaceholderRunner {
				c.placeholderRunners++
			} else {
				c.placeholderWorkflows++
			}
		}
	}

	// Second pass: real workflow (job) pods. The hook stamps runner-pod on the
	// job pod AND on container-step / Docker-action step pods, so matching that
	// label alone counts a runner running a container step as several workflow
	// slots. Select only the job pod by its "-workflow" name suffix (step pods
	// are "<runner>-step-<hex>"), correlate it to a counted runner via the
	// runner-pod label, and credit each runner at most once.
	countedWorkflowRunners := make(map[string]struct{})
	for i := range pods {
		pod := &pods[i]
		if !strings.HasSuffix(pod.Name, workflowPodNameSuffix) {
			continue
		}
		runnerPodName, ok := pod.Labels[labelWorkflowRunnerPod]
		if !ok {
			continue
		}
		if _, isCounted := countedRunnerNames[runnerPodName]; !isCounted {
			continue
		}
		if _, already := countedWorkflowRunners[runnerPodName]; already {
			continue
		}
		if boundNonTerminal(pod) {
			c.realWorkflows++
			countedWorkflowRunners[runnerPodName] = struct{}{}
		}
	}

	// runningPairs is computed off the same snapshot for the running-pairs gauge.
	for _, pair := range groupBySlot(listenerPlaceholders) {
		if pair.BothRunning() {
			c.runningPairs++
		}
	}

	return c
}

// listNamespacePodsWithRetry returns a single snapshot of every pod in the
// runner namespace. Namespace-wide (not scale-set-scoped) because real
// workflow pods carry no scale-set label — only labelWorkflowRunnerPod.
func (m *Monitor) listNamespacePodsWithRetry(ctx context.Context, maxRetries int) ([]corev1.Pod, error) {
	var pods []corev1.Pod
	err := retryWithBackoff(ctx, m.logger, "list-namespace-pods", maxRetries, func() error {
		list, e := m.clientset.CoreV1().Pods(m.config.Namespace).List(ctx, metav1.ListOptions{})
		if e != nil {
			return e
		}
		pods = list.Items
		return nil
	})
	return pods, err
}
