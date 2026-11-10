// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"k8s.io/utils/set"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurmclientfake "github.com/SlinkyProject/slurm-client/pkg/client/fake"
	"github.com/SlinkyProject/slurm-client/pkg/object"

	nodeutils "github.com/SlinkyProject/slurm-bridge/internal/controller/node/utils"
	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

// A Slurm node held for a lost host stays drained while a deleted bridged pod
// is still bound to it, and is then undrained, or kept drained for a cordon.
func TestSyncStateLostHostHold(t *testing.T) {
	stopping := func(scheduler string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod", Finalizers: []string{wellknown.FinalizerScheduler}, DeletionTimestamp: ptr.To(metav1.Now())},
			Spec:       corev1.PodSpec{SchedulerName: scheduler, NodeName: "host"},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}
	for _, tc := range []struct {
		name         string
		pod          *corev1.Pod
		cordoned     bool
		allowUndrain bool
		wantHeld     bool
		wantDrained  string // reason when drained for another cause
		wantRecheck  bool
	}{
		{name: "pod still stopping", pod: stopping(schedulerName, corev1.PodRunning), allowUndrain: true, wantHeld: true, wantRecheck: true},
		{name: "pod finished", pod: stopping(schedulerName, corev1.PodFailed), allowUndrain: true},
		{name: "no pods", allowUndrain: true},
		{name: "another scheduler's pod", pod: stopping("default-scheduler", corev1.PodRunning), allowUndrain: true},
		{name: "cordoned", cordoned: true, allowUndrain: true, wantDrained: "slurm-bridge: Corresponding Kubernetes node (host) is unschedulable"},
		{name: "registration owns the drain", wantHeld: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host"}, Spec: corev1.NodeSpec{Unschedulable: tc.cordoned}}
			builder := fake.NewClientBuilder().WithObjects(node).
				WithIndex(&corev1.Pod{}, nodeutils.IndexFieldPodNodeName, nodeutils.IndexPodByNodeName)
			if tc.pod != nil {
				builder = builder.WithObjects(tc.pod)
			}
			slurmNode := &api.V0044Node{Name: ptr.To("host"), State: ptr.To([]api.V0044NodeState{api.V0044NodeStateDOWN, api.V0044NodeStateDRAIN}), Reason: ptr.To(wellknown.SlurmReasonLostHost)}
			updateFn := func(_ context.Context, obj object.Object, req any, _ ...slurmclient.UpdateOption) error {
				o, ok := obj.(*api.V0044Node)
				r, isMsg := req.(api.V0044UpdateNodeMsg)
				if !ok || !isMsg {
					return errors.New("unexpected update")
				}
				states := set.New(ptr.Deref(o.State, nil)...)
				for _, s := range ptr.Deref(r.State, nil) {
					if s == api.V0044UpdateNodeMsgStateUNDRAIN {
						states.Delete(api.V0044NodeStateDRAIN)
					} else {
						states.Insert(api.V0044NodeState(s))
					}
				}
				o.State, o.Reason = ptr.To(states.UnsortedList()), r.Reason
				return nil
			}
			slurmClient := slurmclientfake.NewClientBuilder().WithUpdateFn(updateFn).WithObjects(slurmNode).Build()
			r := NewReconciler(builder.Build(), slurmClient, schedulerName, make(chan event.GenericEvent), nil)
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "host"}}
			_ = durationStore.Pop(req.String())

			if err := r.syncState(t.Context(), req, tc.allowUndrain); err != nil {
				t.Fatal(err)
			}

			got := &api.V0044Node{}
			if err := slurmClient.Get(t.Context(), object.ObjectKey("host"), got); err != nil {
				t.Fatal(err)
			}
			drained := set.New(ptr.Deref(got.State, nil)...).Has(api.V0044NodeStateDRAIN)
			reason := ptr.Deref(got.Reason, "")
			switch {
			case tc.wantHeld:
				if !drained || reason != wellknown.SlurmReasonLostHost {
					t.Fatalf("drained=%t reason=%q, want held", drained, reason)
				}
			case tc.wantDrained != "":
				if !drained || reason != tc.wantDrained {
					t.Fatalf("drained=%t reason=%q, want drained for %q", drained, reason, tc.wantDrained)
				}
			default:
				if drained {
					t.Fatalf("still drained, reason=%q", reason)
				}
			}
			if recheck := durationStore.Pop(req.String()) > 0; recheck != tc.wantRecheck {
				t.Fatalf("recheck=%t, want %t", recheck, tc.wantRecheck)
			}
		})
	}
}
