// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package registration

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/kubernetes/pkg/util/taints"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodeutils "github.com/SlinkyProject/slurm-bridge/internal/controller/node/utils"
)

const eventReasonStranded = "SlurmAllocationReleasedNodeLost"

// strandedEventKey records, in eventHistory, that a Pod's stranded release was
// reported.
type strandedEventKey types.UID

var unreachableTaint = &corev1.Taint{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoExecute}

// strandedMembers returns the deleted, bound Pods whose host has been lost.
// Such a Pod stays Terminating until its kubelet confirms it, which a dead host
// never does, so it would hold the allocation indefinitely.
//
// Without confirm, it returns candidates from the cache: their kubelet stopped
// reporting (Ready is Unknown). With confirm, read live, the kubelet must
// still be silent, the node must carry the node lifecycle controller's
// NoExecute unreachable taint, and Slurm must report the node as not
// responding. Requiring both the kubelet and slurmd to be silent rules out a
// kubelet failure or an API server partition while the host and slurmd still
// run, where Slurm could otherwise start new work on resources that the Pod's
// containers still use.
//
// Stranded Pods matter only when every other member has finished, so it
// returns none as soon as one member holds the allocation anyway. lost caches
// each node's result for the caller; use one map per mode.
//
// A stranded Pod counts as finished only for releasing the allocation. Its
// finalizer and claims stay until Kubernetes finishes the Pod.
func (r *Reconciler) strandedMembers(ctx context.Context, pods []corev1.Pod, confirm bool, lost map[string]bool) (map[types.UID]bool, error) {
	if !r.ReleaseStranded {
		return nil, nil //nolint:nilnil // A nil set has no stranded members.
	}
	for i := range pods {
		pod := &pods[i]
		if !podReadyForCleanup(pod) && (pod.DeletionTimestamp == nil || pod.Spec.NodeName == "") {
			return nil, nil //nolint:nilnil // A nil set has no stranded members.
		}
	}
	var stranded map[types.UID]bool
	for i := range pods {
		pod := &pods[i]
		if podReadyForCleanup(pod) {
			continue
		}
		hostLost, checked := lost[pod.Spec.NodeName]
		if !checked {
			var err error
			hostLost, err = r.hostLost(ctx, pod.Spec.NodeName, confirm)
			if err != nil {
				return nil, err
			}
			lost[pod.Spec.NodeName] = hostLost
		}
		if !hostLost {
			return nil, nil //nolint:nilnil // A nil set has no stranded members.
		}
		if stranded == nil {
			stranded = make(map[types.UID]bool)
		}
		stranded[pod.UID] = true
	}
	return stranded, nil
}

func (r *Reconciler) hostLost(ctx context.Context, nodeName string, confirm bool) (bool, error) {
	reader := client.Reader(r.Client)
	if confirm && r.apiReader != nil {
		reader = r.apiReader
	}
	node := &corev1.Node{}
	if err := reader.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			// The Pod garbage collector fails the Pods of a deleted node.
			return false, nil
		}
		return false, err
	}
	// A kubelet that reports NotReady is alive and finishes its Pods itself.
	if _, ready := nodeutil.GetNodeCondition(&node.Status, corev1.NodeReady); ready == nil || ready.Status != corev1.ConditionUnknown {
		return false, nil
	}
	if !confirm {
		return true, nil
	}
	if !taints.TaintExists(node.Spec.Taints, unreachableTaint) {
		return false, nil
	}
	return r.slurmControl.IsNodeNotResponding(ctx, nodeutils.GetSlurmNodeName(node))
}

// holdLostHosts drains the Slurm nodes of stranded Pods before their
// allocation is released, so that no new work starts on resources a Pod's
// containers may still use if its host returns. The node controller lifts the
// hold once Kubernetes confirms the Pods stopped.
func (r *Reconciler) holdLostHosts(ctx context.Context, pods []corev1.Pod) error {
	held := make(map[string]bool)
	for i := range pods {
		nodeName := pods[i].Spec.NodeName
		if held[nodeName] {
			continue
		}
		held[nodeName] = true
		node := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if err := r.slurmControl.HoldNode(ctx, nodeutils.GetSlurmNodeName(node)); err != nil {
			return err
		}
	}
	return nil
}

// recordStranded reports, once per Pod, a cancellation that did not wait for
// stranded Pods. The Slurm job then ends as canceled rather than failed.
func (r *Reconciler) recordStranded(pods []corev1.Pod) {
	if r.eventRecorder == nil {
		return
	}
	for i := range pods {
		key := strandedEventKey(pods[i].UID)
		if r.eventHistory != nil {
			if _, found := r.eventHistory.Get(key); found {
				continue
			}
			r.eventHistory.Set(key, true, registrationEventReminder)
		}
		r.eventRecorder.Eventf(&pods[i], corev1.EventTypeWarning, eventReasonStranded,
			"Canceled the Slurm job: node %s stopped responding to Kubernetes and Slurm", pods[i].Spec.NodeName)
	}
}

// strandedPods returns the stranded Pods that a release did not wait for, or
// none when the component was releasable without them.
func strandedPods(pods []corev1.Pod, stranded map[types.UID]bool) []corev1.Pod {
	if len(stranded) == 0 || releasable(pods, nil) {
		return nil
	}
	var out []corev1.Pod
	for i := range pods {
		if stranded[pods[i].UID] {
			out = append(out, pods[i])
		}
	}
	return out
}
