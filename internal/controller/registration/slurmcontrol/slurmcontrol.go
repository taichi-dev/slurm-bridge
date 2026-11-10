// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmcontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	api "github.com/SlinkyProject/slurm-client/api/v0044"
	"github.com/SlinkyProject/slurm-client/pkg/client"
	slurmerrors "github.com/SlinkyProject/slurm-client/pkg/errors"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	slurmutils "github.com/SlinkyProject/slurm-client/pkg/utils"

	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

type SlurmControlInterface interface {
	// IsJobRunning returns true if the Slurm job is running, false if not.
	IsJobRunning(ctx context.Context, pod *corev1.Pod) (bool, error)
	// TerminateJob cancels the Slurm job by JobId
	TerminateJob(ctx context.Context, jobId int32) error
	// TerminateHetJobComponent cancels the component at hetJobOffset.
	// Offset zero cancels the whole heterogeneous allocation.
	TerminateHetJobComponent(ctx context.Context, hetJobId, hetJobOffset int32) error
	// IsNodeNotResponding reports whether Slurm currently reports the node as
	// not responding, read live. A node unknown to Slurm is reported as
	// responding.
	IsNodeNotResponding(ctx context.Context, nodeName string) (bool, error)
	// HoldNode drains the Slurm node with wellknown.SlurmReasonLostHost, so no
	// new work starts on it while a pod on its lost host may still run. A
	// drain set by someone other than slurm-bridge is left as it is, and a
	// node unknown to Slurm is skipped.
	HoldNode(ctx context.Context, nodeName string) error
}

// RealSlurmControl is the default implementation of SlurmControlInterface.
type realSlurmControl struct {
	client.Client
}

// IsJobRunning implements SlurmControlInterface.
func (r *realSlurmControl) IsJobRunning(ctx context.Context, pod *corev1.Pod) (bool, error) {
	job := &api.V0044JobInfo{}
	jobId := object.ObjectKey(pod.Labels[wellknown.LabelExternalJobId])
	if jobId == "" {
		return false, nil
	}
	// Trust the informer's own periodic refresh (bounded staleness of one sync period,
	// the same order as this pod's own reconcile cadence) rather than forcing a live
	// RefreshCache read on every reconcile of every running pod. The common case here is
	// "yes, still running"; a forced refresh pays a synchronous batch+poll cost on the
	// hottest, most frequently repeated path in the controller for no correctness gain.
	err := r.Get(ctx, jobId, job)
	if err != nil && !errors.Is(err, slurmerrors.ErrNotFound) {
		return false, err
	}
	if err == nil && slurmutils.GetStateAsSet(job.JobState).Has(api.V0044JobInfoJobStateRUNNING) {
		return true, nil
	}

	// Any other outcome (not running, or not found) drives Pod deletion downstream, so
	// recheck live before trusting a cached result that would delete the pod.
	err = r.Get(ctx, jobId, job, &client.GetOptions{SkipCache: true})
	if err != nil {
		if errors.Is(err, slurmerrors.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if job.GetKey() != jobId {
		return false, fmt.Errorf("slurm returned job %s when requesting job %s", job.GetKey(), jobId)
	}
	if slurmutils.GetStateAsSet(job.JobState).Has(api.V0044JobInfoJobStateRUNNING) {
		return true, nil
	}
	return false, nil
}

// IsNodeNotResponding implements SlurmControlInterface.
func (r *realSlurmControl) IsNodeNotResponding(ctx context.Context, nodeName string) (bool, error) {
	node := &api.V0044Node{}
	key := object.ObjectKey(nodeName)
	if err := r.Get(ctx, key, node, &client.GetOptions{SkipCache: true}); err != nil {
		if errors.Is(err, slurmerrors.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if node.GetKey() != key {
		return false, fmt.Errorf("slurm returned node %s when requesting node %s", node.GetKey(), key)
	}
	return slurmutils.GetStateAsSet(node.State).Has(api.V0044NodeStateNOTRESPONDING), nil
}

// HoldNode implements SlurmControlInterface.
func (r *realSlurmControl) HoldNode(ctx context.Context, nodeName string) error {
	node := &api.V0044Node{}
	key := object.ObjectKey(nodeName)
	if err := r.Get(ctx, key, node, &client.GetOptions{SkipCache: true}); err != nil {
		if errors.Is(err, slurmerrors.ErrNotFound) {
			return nil
		}
		return err
	}
	if node.GetKey() != key {
		return fmt.Errorf("slurm returned node %s when requesting node %s", node.GetKey(), key)
	}
	states := slurmutils.GetStateAsSet(node.State)
	reason := ptr.Deref(node.Reason, "")
	if states.Has(api.V0044NodeStateDRAIN) && !states.Has(api.V0044NodeStateUNDRAIN) &&
		(reason == wellknown.SlurmReasonLostHost || (reason != "" && !strings.Contains(reason, slurmBridgeReasonPrefix))) {
		return nil
	}
	req := api.V0044UpdateNodeMsg{
		State:  ptr.To([]api.V0044UpdateNodeMsgState{api.V0044UpdateNodeMsgStateDRAIN}),
		Reason: ptr.To(wellknown.SlurmReasonLostHost),
	}
	if err := r.Update(ctx, node, req); err != nil && !errors.Is(err, slurmerrors.ErrNotFound) {
		return err
	}
	return nil
}

// slurmBridgeReasonPrefix starts every node reason the bridge sets.
const slurmBridgeReasonPrefix = "slurm-bridge:"

// TerminateJob implements SlurmControlInterface.
func (r *realSlurmControl) TerminateJob(ctx context.Context, jobId int32) error {
	job := &api.V0044JobInfo{
		JobId: ptr.To(jobId),
	}
	if err := r.Delete(ctx, job, &client.DeleteOptions{SkipReadAfterDelete: true}); err != nil {
		if errors.Is(err, slurmerrors.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}

var _ SlurmControlInterface = &realSlurmControl{}

func NewControl(client client.Client) SlurmControlInterface {
	return &realSlurmControl{
		Client: client,
	}
}
