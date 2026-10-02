package capacity

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// AnyInPhase returns true if either pod in the pair is in phase.
func (p *PlaceholderPair) AnyInPhase(phase corev1.PodPhase) bool {
	return (p.RunnerPod != nil && p.RunnerPod.Status.Phase == phase) ||
		(p.WorkflowPod != nil && p.WorkflowPod.Status.Phase == phase)
}

// CleanupTerminal deletes pairs where either pod Succeeded (its sleep ran out
// before a real pod preempted it) or Failed. Return values as for
// deletePairsWhere.
func (pm *PlaceholderManager) CleanupTerminal(
	ctx context.Context,
	pairs map[string]*PlaceholderPair,
) (int, int, []string) {
	return pm.deletePairsWhere(ctx, pairs,
		func(pair *PlaceholderPair) bool {
			return pair.AnyInPhase(corev1.PodSucceeded) || pair.AnyInPhase(corev1.PodFailed)
		},
		func(slotID string, _ *PlaceholderPair, err error) {
			pm.logger.Warn("failed to delete terminal pair", "slotID", slotID, "error", err)
		},
	)
}

// deletePairsWhere returns every slot it attempted, failed deletes included.
func (pm *PlaceholderManager) deletePairsWhere(
	ctx context.Context,
	pairs map[string]*PlaceholderPair,
	match func(*PlaceholderPair) bool,
	logDeleteError func(slotID string, pair *PlaceholderPair, err error),
) (deleted, errored int, slots []string) {
	for slotID, pair := range pairs {
		if !match(pair) {
			continue
		}
		slots = append(slots, slotID)
		if err := pm.DeletePair(ctx, slotID); err != nil {
			logDeleteError(slotID, pair, err)
			errored++
			continue
		}
		deleted++
	}
	return deleted, errored, slots
}

// cleanupPairs drops every slot cleanup attempted, failed deletes included, from pairs.
func (m *Monitor) cleanupPairs(
	ctx context.Context,
	pairs map[string]*PlaceholderPair,
	reason, msg string,
	cleanup func(context.Context, map[string]*PlaceholderPair) (int, int, []string),
) {
	deleted, errored, slots := cleanup(ctx, pairs)
	for _, slotID := range slots {
		delete(pairs, slotID)
	}
	m.recordPairDeletes(reason, msg, deleted, errored)
}

func (m *Monitor) recordPairDeletes(reason, msg string, deleted, errored int) {
	for i := 0; i < deleted; i++ {
		m.recorder.IncPairDeletes(reason, resultSuccess)
	}
	for i := 0; i < errored; i++ {
		m.recorder.IncPairDeletes(reason, resultError)
	}
	if deleted > 0 || errored > 0 {
		m.logger.Info(msg, "success", deleted, "failed", errored)
	}
}
