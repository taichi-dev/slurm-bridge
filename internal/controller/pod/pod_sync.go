// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package pod

import (
	"context"
	"time"

	"github.com/SlinkyProject/slurm-bridge/internal/utils/slurmjobir"
	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// deletionGraceSlack is how long past its deletion grace period a Pod is given
// before being treated as terminal. It absorbs the normal lag between the grace
// period expiring and kubelet reporting the final phase.
const deletionGraceSlack = 2 * time.Minute

// isCleanable reports whether a Pod's placeholder job and finalizer may be
// released.
//
// Reaching a terminal phase is the normal qualifier. A Pod deleted before its
// containers ever started never reaches one: kubelet tears the containers down
// but leaves the phase at Pending, so IsPodTerminal stays false forever. Gating
// solely on the phase therefore deadlocks - syncSlurm never terminates the
// placeholder job so it keeps holding its allocation, prepareTerminalPod never
// removes the finalizer so the Pod object never goes away, and the
// SlurmJobRunnable sweep cannot break the cycle either because it keys off the
// Pod being gone. Once the deletion grace period has clearly elapsed the Pod is
// not coming back, so treat it as terminal.
func isCleanable(pod *corev1.Pod) bool {
	if podv1.IsPodTerminal(pod) {
		return true
	}
	if pod.DeletionTimestamp == nil {
		return false
	}
	grace := ptr.Deref(pod.DeletionGracePeriodSeconds,
		ptr.Deref(pod.Spec.TerminationGracePeriodSeconds, 0))
	deadline := pod.DeletionTimestamp.Add(time.Duration(grace)*time.Second + deletionGraceSlack)
	return time.Now().After(deadline)
}

func (r *PodReconciler) Sync(ctx context.Context, req reconcile.Request) error {
	var errs []error

	if err := r.syncKubernetes(ctx, req); err != nil {
		errs = append(errs, err)
	}

	if err := r.syncSlurm(ctx, req); err != nil {
		errs = append(errs, err)
	}

	if err := r.prepareTerminalPod(ctx, req); err != nil {
		errs = append(errs, err)
	}

	return utilerrors.NewAggregate(errs)
}

// syncKubernetes reconciles the Kubernetes Pod with Slurm Jobs.
// It will terminate the pod without a corresponding job.
func (r *PodReconciler) syncKubernetes(ctx context.Context, req reconcile.Request) error {
	logger := log.FromContext(ctx)
	podKey := req.String()

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if pod.Spec.SchedulerName != r.SchedulerName {
		logger.V(2).Info("Pod is not scheduled by the slurm-bridge, skipping",
			"pod", klog.KObj(pod), "scheduler", r.SchedulerName)
		return nil
	}

	// Terminal pods are handled by prepareTerminalPod
	if isCleanable(pod) {
		logger.V(2).Info("Pod is terminal, skipping", "pod", klog.KObj(pod))
		return nil
	}

	// Requeue Pod request until terminal. A Pod deleted before its containers
	// started never changes phase again, so this requeue is also what brings it
	// back once its deletion grace period expires and isCleanable() flips.
	durationStore.Push(podKey, 30*time.Second)

	// Unbound pods must be scheduled before checking if the Slurm job is running
	if pod.Spec.NodeName == "" {
		logger.V(2).Info("Pod is not bound, skipping", "pod", klog.KObj(pod))
		return nil
	}

	jobId := slurmjobir.ParseSlurmJobId(pod.Labels[wellknown.LabelExternalJobId])
	exists, err := r.slurmControl.IsJobRunning(ctx, pod)
	if err != nil {
		logger.Error(err, "failed to fetch Slurm job information", "jobId", jobId)
		return err
	}

	if !exists {
		logger.Info("Deleting Pod for corresponding Slurm Job",
			"pod", podKey, "jobId", jobId)
		if err := r.Delete(ctx, pod); err != nil {
			logger.Error(err, "failed to terminate Pod without corresponding Slurm Job",
				"pod", podKey, "jobId", jobId)
			return err
		}
	}

	return nil
}

// syncSlurm reconciles the Slurm Job with Kubernetes Pods.
// It will terminate the job corresponding to a terminated pod.
func (r *PodReconciler) syncSlurm(ctx context.Context, req reconcile.Request) error {
	logger := log.FromContext(ctx)
	podKey := req.String()

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(2).Info("Pod not found, no Job ID", "pod", podKey)
			return nil
		}
		return err
	}

	if !isCleanable(pod) {
		logger.V(2).Info("Pod is not terminated, skipping", "pod", podKey)
		return nil
	}

	podList := &corev1.PodList{}
	listOpts := &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			wellknown.LabelExternalJobId: pod.Labels[wellknown.LabelExternalJobId],
		}),
	}
	if err := r.List(ctx, podList, listOpts); err != nil {
		logger.Error(err, "failed to fetch pods associated with Slurm job")
		return err
	}

	// If there are no non-terminated pods labeled with this jobId the Slurm Job may
	// be terminated. This must use the same predicate as the guard above,
	// otherwise a Pod that is cleanable only because its grace period expired
	// still counts itself as non-terminal and the job is never released.
	nonTerminalPods := 0
	terminatingPods := 0
	for _, p := range podList.Items {
		if isCleanable(&p) {
			continue
		}
		nonTerminalPods++
		if p.DeletionTimestamp != nil {
			terminatingPods++
		}
	}
	jobId := slurmjobir.ParseSlurmJobId(pod.Labels[wellknown.LabelExternalJobId])
	if nonTerminalPods == 0 {
		logger.Info("Terminate Slurm Job for Pod", "pod", klog.KObj(pod), "jobId", jobId)
		if err := r.slurmControl.TerminateJob(ctx, jobId); err != nil {
			logger.Error(err, "failed to terminate Slurm Job without corresponding Pod",
				"jobId", jobId, "pod", podKey)
			return err
		}
	} else if terminatingPods > 0 {
		logger.V(4).Info("Retaining Slurm Job until non-terminating Pods complete",
			"jobId", jobId, "nonTerminalPods", nonTerminalPods, "terminatingPods", terminatingPods)
	}

	return nil
}

// prepareTerminalPod will remove the finalizer and resource claims from the pod
// once the pod reaches a terminal phase (see isCleanable). This is done to
// ensure syncSlurm is able to get the pod labels to determine if the pod has an
// external JobId and cleanup resource claims that were generated by
// slurm-bridge. Sync() runs it after syncSlurm so the finalizer outlives the
// job termination; removing it first would let the Pod disappear and orphan the
// placeholder job.
func (r *PodReconciler) prepareTerminalPod(ctx context.Context, req reconcile.Request) error {
	logger := log.FromContext(ctx)
	podKey := req.String()

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(2).Info("Pod not found, no finalizer to remove", "pod", podKey)
			return nil
		}
		return err
	}

	if !isCleanable(pod) {
		logger.V(2).Info("Pod is not terminated, skipping", "pod", podKey)
		return nil
	}

	finalizers := []string{}
	for _, f := range pod.Finalizers {
		if f != wellknown.FinalizerScheduler {
			finalizers = append(finalizers, f)
		}
	}
	toUpdate := pod.DeepCopy()
	toUpdate.Finalizers = finalizers
	if err := r.Patch(ctx, toUpdate, client.StrategicMergeFrom(pod)); err != nil {
		logger.Error(err, "failed to remove finalizer", "pod", podKey)
		return err
	}

	if pod.Status.ExtendedResourceClaimStatus != nil &&
		pod.Status.ExtendedResourceClaimStatus.ResourceClaimName != "" {
		claimKey := client.ObjectKey{
			Name:      pod.Status.ExtendedResourceClaimStatus.ResourceClaimName,
			Namespace: pod.Namespace,
		}
		var claim resourcev1.ResourceClaim
		if err := r.Get(ctx, claimKey, &claim); err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to get resource claim", "claimKey", claimKey)
				return err
			}
		} else {
			if err := r.Delete(ctx, &claim); err != nil {
				logger.Error(err, "failed to delete resource claim", "claim", claim)
				return err
			}
		}
	}

	return nil
}
