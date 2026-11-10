// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package registration

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kubeinterceptor "sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"

	api "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurminterceptor "github.com/SlinkyProject/slurm-client/pkg/client/interceptor"
	"github.com/SlinkyProject/slurm-client/pkg/object"

	"github.com/SlinkyProject/slurm-bridge/internal/controller/registration/slurmcontrol"
	"github.com/SlinkyProject/slurm-bridge/internal/utils/slurmjobir"
	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

// newStrandedFixture is a lifecycle fixture with stranded release enabled,
// which records the nodes held for a lost host and how many cancellations
// preceded each hold.
func newStrandedFixture(t *testing.T) *lifecycleFixture {
	t.Helper()
	f := newLifecycleFixture(t)
	f.r.ReleaseStranded = true
	inner := f.r.slurmClient
	f.r.slurmControl = slurmcontrol.NewControl(slurminterceptor.NewClient(inner, slurminterceptor.Funcs{
		Update: func(ctx context.Context, obj object.Object, req any, opts ...slurmclient.UpdateOption) error {
			if msg, ok := req.(api.V0044UpdateNodeMsg); ok && ptr.Deref(msg.Reason, "") == wellknown.SlurmReasonLostHost {
				if f.holdErr != nil {
					return f.holdErr
				}
				f.holds = append(f.holds, heldNode{name: string(obj.GetKey()), cancelsBefore: len(f.cancellations)})
			}
			return inner.Update(ctx, obj, req, opts...)
		},
	}))
	return f
}

type heldNode struct {
	name          string
	cancelsBefore int
}

type strandedHost struct {
	missing     bool
	ready       corev1.ConditionStatus
	unreachable bool
	notReady    bool // the node lifecycle controller's not-ready taint
}

func (f *lifecycleFixture) addNode(t *testing.T, name string, host strandedHost) {
	t.Helper()
	if host.missing {
		return
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{wellknown.LabelSlurmNodeName: "slurm-" + name}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: host.ready}}}}
	if host.unreachable {
		node.Spec.Taints = []corev1.Taint{
			{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
			{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoExecute},
		}
	}
	if host.notReady {
		node.Spec.Taints = []corev1.Taint{
			{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
			{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute},
		}
	}
	if err := f.kube.Create(t.Context(), node); err != nil {
		t.Fatal(err)
	}
}

func (f *lifecycleFixture) addSlurmNode(t *testing.T, name string, states ...api.V0044NodeState) {
	t.Helper()
	node := &api.V0044Node{Name: ptr.To(name), State: ptr.To(states)}
	if err := f.live.Create(t.Context(), node, nil); err != nil {
		t.Fatal(err)
	}
}

// runMember binds a member to nodeName and marks it running, optionally deleted.
func (f *lifecycleFixture) runMember(t *testing.T, pod *corev1.Pod, nodeName string, deleted bool) {
	t.Helper()
	current := &corev1.Pod{}
	if err := f.kube.Get(t.Context(), client.ObjectKeyFromObject(pod), current); err != nil {
		t.Fatal(err)
	}
	current.Spec.NodeName = nodeName
	if err := f.kube.Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = corev1.PodRunning
	if err := f.kube.Status().Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if deleted {
		if err := f.kube.Delete(t.Context(), current); err != nil {
			t.Fatal(err)
		}
	}
}

// runLifecycle reconciles the allocation, which must not wait for cleanup.
func (f *lifecycleFixture) runLifecycle(t *testing.T) {
	t.Helper()
	pods, _, err := f.r.members(t.Context(), objectRequest(f.group))
	if err != nil && !errors.Is(err, slurmjobir.ErrorPodGroupNoPods) {
		t.Fatal(err)
	}
	if waiting, err := f.r.reconcileLifecycle(t.Context(), objectRequest(f.group), pods, f.jobs); err != nil || waiting {
		t.Fatalf("waiting=%t, error=%v", waiting, err)
	}
}

func TestLifecycleStrandedMember(t *testing.T) {
	notResponding := []api.V0044NodeState{api.V0044NodeStateDOWN, api.V0044NodeStateNOTRESPONDING}
	responding := []api.V0044NodeState{api.V0044NodeStateDOWN}
	lost := strandedHost{ready: corev1.ConditionUnknown, unreachable: true}
	for _, tc := range []struct {
		name       string
		host       strandedHost
		slurm      []api.V0044NodeState // nil: Slurm does not know the node
		notDeleted bool
		release    bool
	}{
		{name: "kubelet and slurmd silent", host: lost, slurm: notResponding, release: true},
		// A kubelet failure or API server partition: slurmd still runs, so Slurm
		// could start new work on resources the containers may still use.
		{name: "slurmd responding", host: lost, slurm: responding},
		{name: "unknown to Slurm", host: lost},
		// The node lifecycle controller has not tainted the node yet.
		{name: "not yet tainted", host: strandedHost{ready: corev1.ConditionUnknown}, slurm: notResponding},
		// The kubelet is alive and finishes the Pod itself.
		{name: "kubelet ready", host: strandedHost{ready: corev1.ConditionTrue}, slurm: notResponding},
		{name: "kubelet not ready", host: strandedHost{ready: corev1.ConditionFalse, notReady: true}, slurm: notResponding},
		// The kubelet is back before the node lifecycle controller removed the taint.
		{name: "kubelet recovered", host: strandedHost{ready: corev1.ConditionTrue, unreachable: true}, slurm: notResponding},
		// Kubernetes, not the bridge, decides whether to evict the Pod.
		{name: "not deleted", host: lost, slurm: notResponding, notDeleted: true},
		// The Pod garbage collector fails Pods of a deleted node.
		{name: "node deleted", host: strandedHost{missing: true}, slurm: notResponding},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStrandedFixture(t)
			claim := &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "claim", UID: "claim-uid", Labels: map[string]string{wellknown.LabelAllocationPodUID: string(f.pod.UID)}, OwnerReferences: []metav1.OwnerReference{{Kind: "Pod", UID: f.pod.UID}}}}
			if err := f.kube.Create(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			f.addNode(t, "host", tc.host)
			if tc.slurm != nil {
				f.addSlurmNode(t, "slurm-host", tc.slurm...)
			}
			f.runMember(t, f.pod, "host", !tc.notDeleted)

			f.runLifecycle(t)
			if want := map[bool][]int32{true: {100}}[tc.release]; !slices.Equal(f.cancellations, want) {
				t.Fatalf("cancellations=%v, want %v", f.cancellations, want)
			}
			// The node is held before the job is canceled, and only then.
			if want := map[bool][]heldNode{true: {{name: "slurm-host"}}}[tc.release]; !slices.Equal(f.holds, want) {
				t.Fatalf("holds=%v, want %v", f.holds, want)
			}
			// Kubernetes still has to finish the Pod: its finalizer and claims stay.
			current := &corev1.Pod{}
			if err := f.kube.Get(t.Context(), client.ObjectKeyFromObject(f.pod), current); err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(current.Finalizers, wellknown.FinalizerScheduler) {
				t.Fatalf("finalizer removed: %v", current.Finalizers)
			}
			if err := f.kube.Get(t.Context(), client.ObjectKeyFromObject(claim), &resourcev1.ResourceClaim{}); err != nil {
				t.Fatalf("claim removed: %v", err)
			}
		})
	}
}

// A stranded member does not release the allocation while another member
// still runs on a healthy node; it is released once that member finishes.
func TestLifecycleStrandedMemberWaitsForSurvivors(t *testing.T) {
	f := newStrandedFixture(t)
	f.addNode(t, "host", strandedHost{ready: corev1.ConditionUnknown, unreachable: true})
	f.addNode(t, "healthy", strandedHost{ready: corev1.ConditionTrue})
	f.addSlurmNode(t, "slurm-host", api.V0044NodeStateDOWN, api.V0044NodeStateNOTRESPONDING)
	f.addSlurmNode(t, "slurm-healthy", api.V0044NodeStateALLOCATED)
	f.runMember(t, f.pod, "host", true)

	survivor := f.pod.DeepCopy()
	survivor.Name, survivor.UID, survivor.ResourceVersion = "survivor", "survivor-uid", ""
	survivor.DeletionTimestamp = nil
	survivor.Spec.NodeName = "healthy"
	survivor.Status.Phase = corev1.PodRunning
	if err := f.kube.Create(t.Context(), survivor); err != nil {
		t.Fatal(err)
	}

	f.runLifecycle(t)
	if len(f.cancellations) != 0 {
		t.Fatalf("released while a member runs: %v", f.cancellations)
	}

	current := &corev1.Pod{}
	if err := f.kube.Get(t.Context(), client.ObjectKeyFromObject(survivor), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = corev1.PodSucceeded
	if err := f.kube.Status().Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	f.runLifecycle(t)
	if !slices.Equal(f.cancellations, []int32{100}) {
		t.Fatalf("cancellations=%v after the last running member finished", f.cancellations)
	}
}

// The release event is recorded once, however often the cancellation is
// retried before Slurm reports the job ended.
func TestLifecycleStrandedEventOnce(t *testing.T) {
	f := newStrandedFixture(t)
	recorder := record.NewFakeRecorder(10)
	f.r.eventRecorder, f.r.eventHistory = recorder, cache.NewExpiring()
	f.addNode(t, "host", strandedHost{ready: corev1.ConditionUnknown, unreachable: true})
	f.addSlurmNode(t, "slurm-host", api.V0044NodeStateDOWN, api.V0044NodeStateNOTRESPONDING)
	f.runMember(t, f.pod, "host", true)

	f.acknowledgeOnly = true
	f.runLifecycle(t)
	f.runLifecycle(t)
	if !slices.Equal(f.cancellations, []int32{100, 100}) {
		t.Fatalf("cancellations=%v, want two attempts", f.cancellations)
	}
	if len(recorder.Events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(recorder.Events))
	}
	if event := <-recorder.Events; !strings.Contains(event, eventReasonStranded) {
		t.Fatalf("unexpected event %q", event)
	}
}

// A release that did not need to skip a stranded member does not blame it.
func TestStrandedPodsOnlyWhenNeeded(t *testing.T) {
	lost := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "lost", UID: "lost-uid"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	stranded := map[types.UID]bool{lost.UID: true}
	if got := strandedPods([]corev1.Pod{lost}, stranded); len(got) != 1 {
		t.Fatalf("stranded pods=%v, want the lost pod", got)
	}
	// LWS releases its allocation once any member has finished.
	finished := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "finished", UID: "finished-uid", Labels: map[string]string{lwsv1.SetNameLabelKey: "lws"}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	if got := strandedPods([]corev1.Pod{lost, finished}, stranded); len(got) != 0 {
		t.Fatalf("stranded pods=%v, want none for an LWS release", got)
	}
}

// The live read confirms the kubelet is still silent: a cached candidate
// whose kubelet is back keeps the allocation.
func TestLifecycleStrandedKubeletBackLive(t *testing.T) {
	f := newStrandedFixture(t)
	f.addNode(t, "host", strandedHost{ready: corev1.ConditionUnknown, unreachable: true})
	f.addSlurmNode(t, "slurm-host", api.V0044NodeStateDOWN, api.V0044NodeStateNOTRESPONDING)
	f.runMember(t, f.pod, "host", true)
	f.r.apiReader = kubeinterceptor.NewClient(f.kube.(client.WithWatch), kubeinterceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if node, ok := obj.(*corev1.Node); ok {
				node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			}
			return nil
		},
	})

	f.runLifecycle(t)
	if len(f.cancellations) != 0 {
		t.Fatalf("canceled although the kubelet is back: %v", f.cancellations)
	}
}

// Finding a member that holds the allocation anyway stops the live reads.
func TestStrandedMembersStopsEarly(t *testing.T) {
	f := newStrandedFixture(t)
	f.addNode(t, "a", strandedHost{ready: corev1.ConditionTrue})
	f.addNode(t, "b", strandedHost{ready: corev1.ConditionTrue})
	reads := 0
	f.r.apiReader = kubeinterceptor.NewClient(f.kube.(client.WithWatch), kubeinterceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				reads++
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	deleting := func(name, node string) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), DeletionTimestamp: ptr.To(metav1.Now())},
			Spec: corev1.PodSpec{SchedulerName: schedulerName, NodeName: node}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	}
	running := deleting("running", "a")
	running.DeletionTimestamp = nil

	for _, tc := range []struct {
		name  string
		pods  []corev1.Pod
		reads int
	}{
		{name: "a member is not deleted", pods: []corev1.Pod{deleting("x", "a"), running}, reads: 0},
		{name: "first node is healthy", pods: []corev1.Pod{deleting("x", "a"), deleting("y", "b")}, reads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads = 0
			stranded, err := f.r.strandedMembers(t.Context(), tc.pods, true, make(map[string]bool))
			if err != nil || len(stranded) != 0 || reads != tc.reads {
				t.Fatalf("stranded=%v, error=%v, reads=%d, want none and %d reads", stranded, err, reads, tc.reads)
			}
		})
	}
}

// The allocation is not released when the node cannot be held.
func TestLifecycleStrandedHoldFails(t *testing.T) {
	f := newStrandedFixture(t)
	f.holdErr = errors.New("Slurm unavailable")
	f.addNode(t, "host", strandedHost{ready: corev1.ConditionUnknown, unreachable: true})
	f.addSlurmNode(t, "slurm-host", api.V0044NodeStateDOWN, api.V0044NodeStateNOTRESPONDING)
	f.runMember(t, f.pod, "host", true)

	pods, _, err := f.r.members(t.Context(), objectRequest(f.group))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.reconcileLifecycle(t.Context(), objectRequest(f.group), pods, f.jobs); err == nil {
		t.Fatal("expected the hold failure")
	}
	if len(f.cancellations) != 0 {
		t.Fatalf("canceled without holding the node: %v", f.cancellations)
	}
}

// Without the node controller, which lifts holds, nothing is released early.
func TestLifecycleStrandedNeedsNodeController(t *testing.T) {
	f := newStrandedFixture(t)
	f.r.ReleaseStranded = false
	f.addNode(t, "host", strandedHost{ready: corev1.ConditionUnknown, unreachable: true})
	f.addSlurmNode(t, "slurm-host", api.V0044NodeStateDOWN, api.V0044NodeStateNOTRESPONDING)
	f.runMember(t, f.pod, "host", true)

	f.runLifecycle(t)
	if len(f.cancellations) != 0 || len(f.holds) != 0 {
		t.Fatalf("cancellations=%v, holds=%v, want none", f.cancellations, f.holds)
	}
}
