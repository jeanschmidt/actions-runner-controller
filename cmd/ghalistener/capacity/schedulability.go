package capacity

import (
	"context"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// nodeInformerResync of 0 disables periodic full relists; the watch keeps
// the node cache current, which is all the schedulability check reads.
const nodeInformerResync = 0

// nodeSchedulabilityWatcher owns the shared node informer/lister backing the
// placeholder schedulability check. A nil *nodeSchedulabilityWatcher is the
// disabled state; all methods are nil-safe so callers need not branch.
type nodeSchedulabilityWatcher struct {
	factory   informers.SharedInformerFactory
	lister    corev1listers.NodeLister
	hasSynced cache.InformerSynced
}

// newNodeSchedulabilityWatcher returns a watcher when enabled, else nil.
// Constructing the informer here (not in Run) lets the lister be wired before
// the reporter goroutine starts.
func newNodeSchedulabilityWatcher(clientset kubernetes.Interface, enabled bool) *nodeSchedulabilityWatcher {
	if !enabled {
		return nil
	}
	factory := informers.NewSharedInformerFactory(clientset, nodeInformerResync)
	nodes := factory.Core().V1().Nodes()
	// Referencing Informer() registers it so factory.Start actually starts it.
	informer := nodes.Informer()
	return &nodeSchedulabilityWatcher{
		factory:   factory,
		lister:    nodes.Lister(),
		hasSynced: informer.HasSynced,
	}
}

// start launches the informer without blocking on the cache sync. Blocking
// would stall listener startup for up to the sync latency, and a one-shot
// wait would latch the check off if the node cache synced late (e.g. RBAC
// applied after the listener started). Each reporter cycle instead consults
// synced() live, so counting degrades until the cache catches up and then
// self-heals.
func (w *nodeSchedulabilityWatcher) start(ctx context.Context, logger *slog.Logger) {
	if w == nil {
		return
	}
	w.factory.Start(ctx.Done())
	logger.Info("node informer started; placeholder schedulability check engages once the node cache syncs")
}

// synced reports whether the node cache is currently populated. Consulted
// live each reporter cycle (not latched) so a late sync self-heals. Nil-safe:
// a disabled watcher is never synced.
func (w *nodeSchedulabilityWatcher) synced() bool {
	return w != nil && w.hasSynced()
}

// placeholderSchedulabilityChecker returns the gate applied to placeholders
// during counting, or nil for core mode (every Running placeholder counts).
// Returns nil — and records a skip — when the check is enabled but the node
// cache is unavailable, so a node-read outage under-refines rather than
// under-reports the whole fleet.
func (m *Monitor) placeholderSchedulabilityChecker() placeholderSchedulabilityFunc {
	if !m.config.EnableNodeSchedulabilityCheck {
		return nil
	}
	if !m.nodeWatcher.synced() {
		m.recorder.IncReconcileSkips(skipReasonReporterNodeCacheUnsynced)
		m.logger.Warn("node cache not synced this cycle; capacity counter degraded to core mode (schedulability check skipped)")
		return nil
	}
	lister := m.nodeWatcher.lister
	return func(pod *corev1.Pod) bool {
		if pod.Spec.NodeName == "" {
			return false
		}
		node, err := lister.Get(pod.Spec.NodeName)
		if err != nil {
			// Node absent from cache: fail safe by excluding — never advertise
			// a reserve whose node we cannot verify.
			return false
		}
		return nodeSchedulableForReplacement(node, pod)
	}
}

// nodeSchedulableForReplacement reports whether node can still host the real
// pod that would replace placeholder pod. The placeholder's whole value is
// that a real pod lands where it sits; a cordon (Spec.Unschedulable) or any
// NoSchedule/NoExecute taint the placeholder does not tolerate makes that
// impossible, so the reserve is invalid and must stop counting.
//
// Keying on taint effect + toleration (not a hardcoded Karpenter key) handles
// cordon, karpenter.sh/disrupted, and the not-ready taint uniformly and
// version-independently. The placeholder mirrors the real pod's scheduling, so
// its own tolerations are the correct proxy for "would the replacement land?".
func nodeSchedulableForReplacement(node *corev1.Node, pod *corev1.Pod) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for i := range node.Spec.Taints {
		taint := &node.Spec.Taints[i]
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if !tolerationsTolerateTaint(pod.Spec.Tolerations, taint) {
			return false
		}
	}
	return true
}

// tolerationsTolerateTaint reports whether any toleration tolerates taint.
func tolerationsTolerateTaint(tolerations []corev1.Toleration, taint *corev1.Taint) bool {
	for i := range tolerations {
		if tolerationToleratesTaint(&tolerations[i], taint) {
			return true
		}
	}
	return false
}

// tolerationToleratesTaint mirrors Kubernetes' Toleration.ToleratesTaint for
// the Exists/Equal operators. It is reimplemented rather than called because
// the upstream method requires a klog.Logger and a feature-gate flag for the
// alpha Lt/Gt numeric operators that placeholder pods never emit.
func tolerationToleratesTaint(t *corev1.Toleration, taint *corev1.Taint) bool {
	if len(t.Effect) > 0 && t.Effect != taint.Effect {
		return false
	}
	if len(t.Key) > 0 && t.Key != taint.Key {
		return false
	}
	switch t.Operator {
	case "", corev1.TolerationOpEqual:
		return t.Value == taint.Value
	case corev1.TolerationOpExists:
		return true
	default:
		return false
	}
}
