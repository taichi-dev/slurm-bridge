// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-FileCopyrightText: Copyright 2024 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package slurmbridge

import (
	"context"
	"errors"
	"testing"

	"github.com/SlinkyProject/slurm-bridge/internal/nodeinfo"
	"github.com/SlinkyProject/slurm-bridge/internal/scheduler/plugins/slurmbridge/slurmcontrol"
	"github.com/SlinkyProject/slurm-bridge/internal/utils/bitmaputil"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	fwk "k8s.io/kube-scheduler/framework"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func init() {
	utilruntime.Must(scheme.AddToScheme(scheme.Scheme))
	utilruntime.Must(resourcev1.AddToScheme(scheme.Scheme))
}

func resourceSliceNodeIndex(obj client.Object) []string {
	rs, ok := obj.(*resourcev1.ResourceSlice)
	if !ok {
		return nil
	}
	nodeName := ptr.Deref(rs.Spec.NodeName, "")
	if nodeName == "" {
		return nil
	}
	return []string{nodeName}
}

func TestSlurmBridge_createRequestsAndMappings(t *testing.T) {
	ctx := context.Background()
	cs := clientsetfake.NewClientset(&resourcev1.DeviceClassList{
		Items: []resourcev1.DeviceClass{
			{ObjectMeta: metav1.ObjectMeta{Name: "foo"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "gpu.example.com"}},
		},
	})
	informerFactory := informers.NewSharedInformerFactory(cs, 0)
	registeredPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
	}
	f, err := tf.NewFramework(
		ctx,
		registeredPlugins,
		"slurm-bridge",
		fwkruntime.WithClientSet(cs),
		fwkruntime.WithInformerFactory(informerFactory),
		fwkruntime.WithSnapshotSharedLister(internalcache.NewSnapshot(
			[]*corev1.Pod{},
			[]*corev1.Node{},
		)))
	if err != nil {
		t.Fatal(err)
	}
	type fields struct {
		Client        client.Client
		schedulerName string
		slurmControl  slurmcontrol.SlurmControlInterface
		handle        fwk.Handle
		gpuTypeMap    map[string]string
	}
	type args struct {
		ctx       context.Context
		pod       *corev1.Pod
		nodeName  string
		resources *slurmcontrol.NodeResources
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErr      bool
		wantRequests int
	}{
		{
			name: "No matching device class name",
			fields: fields{
				Client: fake.NewClientBuilder().
					WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
					Build(),
				handle: f,
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: metav1.NamespaceDefault,
						Name:      "foo",
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "foo",
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU: resource.MustParse("4"),
										corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU: resource.MustParse("4"),
										corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
									},
								},
							},
						},
					},
				},
				nodeName: "node1",
				resources: &slurmcontrol.NodeResources{
					Node:       "node1",
					CoreBitmap: bitmaputil.String(bitmaputil.New(0, 1)),
					Gres: []slurmcontrol.GresLayout{
						{
							Name:  "gpu",
							Type:  "example.com",
							Count: 4,
							Index: "0-3",
						},
					},
				},
			},
		},
		{
			name: "Matching device class name",
			fields: fields{
				handle: f,
				Client: fake.NewClientBuilder().
					WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
					WithObjects(
						&corev1.Node{
							ObjectMeta: metav1.ObjectMeta{Name: "node1"},
						}, &resourcev1.DeviceClass{
							ObjectMeta: metav1.ObjectMeta{
								Name: nodeinfo.DraDriverCpu,
							},
						},
						&resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "node1-cpu",
							},
							Spec: resourcev1.ResourceSliceSpec{
								NodeName: ptr.To("node1"),
								Driver:   nodeinfo.DraDriverCpu,
								Devices: []resourcev1.Device{
									{
										Name: "cpu0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											nodeinfo.DraDriverCpu_CpuID:    {IntValue: ptr.To[int64](0)},
											nodeinfo.DraDriverCpu_CoreID:   {IntValue: ptr.To[int64](0)},
											nodeinfo.DraDriverCpu_SocketID: {IntValue: ptr.To[int64](0)},
											nodeinfo.DraDriverCpu_CoreType: {IntValue: ptr.To(int64(nodeinfo.CoreTypeStandard))},
										},
									},
									{
										Name: "cpu1",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											nodeinfo.DraDriverCpu_CpuID:    {IntValue: ptr.To[int64](1)},
											nodeinfo.DraDriverCpu_CoreID:   {IntValue: ptr.To[int64](0)},
											nodeinfo.DraDriverCpu_SocketID: {IntValue: ptr.To[int64](0)},
											nodeinfo.DraDriverCpu_CoreType: {IntValue: ptr.To(int64(nodeinfo.CoreTypeStandard))},
										},
									},
									{
										Name: "cpu2",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											nodeinfo.DraDriverCpu_CpuID:    {IntValue: ptr.To[int64](2)},
											nodeinfo.DraDriverCpu_CoreID:   {IntValue: ptr.To[int64](1)},
											nodeinfo.DraDriverCpu_SocketID: {IntValue: ptr.To[int64](0)},
											nodeinfo.DraDriverCpu_CoreType: {IntValue: ptr.To(int64(nodeinfo.CoreTypeStandard))},
										},
									},
									{
										Name: "cpu3",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											nodeinfo.DraDriverCpu_CpuID:    {IntValue: ptr.To[int64](3)},
											nodeinfo.DraDriverCpu_CoreID:   {IntValue: ptr.To[int64](1)},
											nodeinfo.DraDriverCpu_SocketID: {IntValue: ptr.To[int64](0)},
											nodeinfo.DraDriverCpu_CoreType: {IntValue: ptr.To(int64(nodeinfo.CoreTypeStandard))},
										},
									},
								},
							},
						},
						&resourcev1.DeviceClass{
							ObjectMeta: metav1.ObjectMeta{
								Name: nodeinfo.DraExampleDriver,
							},
						},
						&resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "node1-gpu",
							},
							Spec: resourcev1.ResourceSliceSpec{
								NodeName: ptr.To("node1"),
								Driver:   nodeinfo.DraExampleDriver,
								Devices: []resourcev1.Device{
									{
										Name: "gpu-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](0)},
										},
									},
									{
										Name: "gpu-1",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](1)},
										},
									},
									{
										Name: "gpu-2",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](2)},
										},
									},
									{
										Name: "gpu-3",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](3)},
										},
									},
								},
							},
						},
					).
					Build(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: metav1.NamespaceDefault,
						Name:      "foo",
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "foo",
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU: resource.MustParse("4"),
										corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU: resource.MustParse("4"),
										corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
									},
								},
							},
						},
					},
				},
				nodeName: "node1",
				resources: &slurmcontrol.NodeResources{
					Node:       "node1",
					CoreBitmap: bitmaputil.String(bitmaputil.New(0, 1)),
					Gres: []slurmcontrol.GresLayout{
						{
							Name:  "gpu",
							Type:  "gpu.example.com",
							Count: 3,
							Index: "0,2-3",
						},
					},
				},
			},
			wantRequests: 2,
		},
		{
			// AutoDetect=nvidia reports GRES type "nvidia_b200"; gpuTypeMap
			// resolves it to the gpu.nvidia.com DeviceClass so the claim and its
			// request mapping are still created.
			name: "AutoDetect model type mapped to gpu.nvidia.com",
			fields: fields{
				handle:     f,
				gpuTypeMap: map[string]string{"nvidia_b200": nodeinfo.DraDriverGpuNvidia},
				Client: fake.NewClientBuilder().
					WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
					WithObjects(
						&corev1.Node{
							ObjectMeta: metav1.ObjectMeta{Name: "node1"},
						},
						&resourcev1.DeviceClass{
							ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia},
						},
						&resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{Name: "node1-gpu"},
							Spec: resourcev1.ResourceSliceSpec{
								NodeName: ptr.To("node1"),
								Driver:   nodeinfo.DraDriverGpuNvidia,
								Devices: []resourcev1.Device{
									{Name: "gpu-0"},
									{Name: "gpu-1"},
								},
							},
						},
					).
					Build(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: metav1.NamespaceDefault,
						Name:      "foo",
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "foo",
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.nvidia.com"): resource.MustParse("2"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.nvidia.com"): resource.MustParse("2"),
									},
								},
							},
						},
					},
				},
				nodeName: "node1",
				resources: &slurmcontrol.NodeResources{
					Node: "node1",
					Gres: []slurmcontrol.GresLayout{
						{
							Name:  "gpu",
							Type:  "nvidia_b200",
							Count: 2,
							Index: "0-1",
						},
					},
				},
			},
			wantRequests: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := &SlurmBridge{
				Client:        tt.fields.Client,
				schedulerName: tt.fields.schedulerName,
				slurmControl:  tt.fields.slurmControl,
				handle:        tt.fields.handle,
				gpuTypeMap:    tt.fields.gpuTypeMap,
			}
			gotClaim, gotMappings, err := sb.createRequestsAndMappings(tt.args.ctx, tt.args.pod, tt.args.nodeName, tt.args.resources)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotClaim == nil {
				return
			}
			if len(gotClaim.Spec.Devices.Requests) != tt.wantRequests {
				t.Errorf("SlurmBridge.createRequestsAndMappings() len(gotClaim.Spec.Devices.Requests) = %v, want %v", len(gotClaim.Spec.Devices.Requests), tt.wantRequests)
			}
			// Every requestMapping MUST reference a device-request that actually
			// exists in the claim by name — the kubelet joins them by name to do
			// CDI injection. A mismatch leaves the device allocated but never
			// injected into the container (see createRequestsAndMappings).
			claimReqNames := map[string]bool{}
			for _, r := range gotClaim.Spec.Devices.Requests {
				claimReqNames[r.Name] = true
			}
			for _, m := range gotMappings {
				if !claimReqNames[m.RequestName] {
					t.Errorf("requestMapping %+v references RequestName %q absent from claim requests %v", m, m.RequestName, claimReqNames)
				}
			}
		})
	}
}

// TestSlurmBridge_createRequestsAndMappings_clampsWholeNode locks in the fix for
// the PodGroup gang over-allocation bug: when Slurm allocates a node exclusively
// (whole-node), its NodeResourceLayout reports the node's ENTIRE GPU pool for the
// job. The generated ResourceClaim must still request only the GPU count the pod
// itself asked for, pinned to exactly that many devices.
func TestSlurmBridge_createRequestsAndMappings_clampsWholeNode(t *testing.T) {
	ctx := context.Background()

	// Node has 8 GPUs published via the NVIDIA DRA driver.
	devices := make([]resourcev1.Device, 0, 8)
	for i := 0; i < 8; i++ {
		devices = append(devices, resourcev1.Device{Name: "gpu-" + string(rune('0'+i))})
	}
	kclient := fake.NewClientBuilder().
		WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
		WithObjects(
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			&resourcev1.DeviceClass{ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia}},
			&resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "node1-gpu"},
				Spec: resourcev1.ResourceSliceSpec{
					NodeName: ptr.To("node1"),
					Driver:   nodeinfo.DraDriverGpuNvidia,
					Devices:  devices,
				},
			},
		).
		Build()

	// Pod (a PodGroup gang member) asks for exactly 1 GPU.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "gang-pod"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "main",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.nvidia.com"): resource.MustParse("1"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.nvidia.com"): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	// Slurm reports the WHOLE node (count=8, indices 0-7) — the gang bug input.
	resources := &slurmcontrol.NodeResources{
		Node: "node1",
		Gres: []slurmcontrol.GresLayout{
			{Name: "gpu", Type: "h100", Count: 8, Index: "0-7"},
		},
	}

	sb := &SlurmBridge{
		Client:     kclient,
		gpuTypeMap: map[string]string{"h100": nodeinfo.DraDriverGpuNvidia},
	}

	claim, mappings, err := sb.createRequestsAndMappings(ctx, pod, "node1", resources)
	if err != nil {
		t.Fatalf("createRequestsAndMappings() error = %v", err)
	}
	if claim == nil {
		t.Fatal("createRequestsAndMappings() claim = nil, want non-nil")
	}
	if len(claim.Spec.Devices.Requests) != 1 {
		t.Fatalf("got %d device requests, want 1", len(claim.Spec.Devices.Requests))
	}
	req := claim.Spec.Devices.Requests[0]
	if req.Exactly == nil {
		t.Fatal("device request Exactly = nil")
	}
	if req.Exactly.Count != 1 {
		t.Errorf("device request Count = %d, want 1 (pod asked for 1 GPU, not the node's 8)", req.Exactly.Count)
	}
	wantExpr := "device.attributes['gpu.nvidia.com'].name in ['gpu-0']"
	if len(req.Exactly.Selectors) != 1 || req.Exactly.Selectors[0].CEL == nil ||
		req.Exactly.Selectors[0].CEL.Expression != wantExpr {
		t.Errorf("device request selector = %+v, want CEL %q", req.Exactly.Selectors, wantExpr)
	}
	// Mapping should still be emitted and reference the (clamped) gpu request.
	if len(mappings) != 1 || mappings[0].RequestName != "gpu" {
		t.Errorf("mappings = %+v, want one mapping with RequestName=gpu", mappings)
	}
}

// TestSlurmBridge_createRequestsAndMappings_multiPodDistinctNodes models a
// multi-pod gang (PodGroup/LWS) where the pods share one Slurm job but land on
// distinct nodes (every translator pins TasksPerNode=1). Slurm reports the WHOLE
// node to each pod, and the pods request different GPU counts. Each pod's claim
// must be clamped independently to that pod's own request against its own node —
// proving co-scheduled gang members never collide on or over-claim GPUs.
func TestSlurmBridge_createRequestsAndMappings_multiPodDistinctNodes(t *testing.T) {
	ctx := context.Background()

	gpuSlice := func(node string) *resourcev1.ResourceSlice {
		devices := make([]resourcev1.Device, 0, 8)
		for i := 0; i < 8; i++ {
			devices = append(devices, resourcev1.Device{Name: "gpu-" + string(rune('0'+i))})
		}
		return &resourcev1.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{Name: node + "-gpu"},
			Spec: resourcev1.ResourceSliceSpec{
				NodeName: ptr.To(node),
				Driver:   nodeinfo.DraDriverGpuNvidia,
				Devices:  devices,
			},
		}
	}
	kclient := fake.NewClientBuilder().
		WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
		WithObjects(
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2"}},
			&resourcev1.DeviceClass{ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia}},
			gpuSlice("node1"),
			gpuSlice("node2"),
		).
		Build()

	gangPod := func(name string, gpus int64) *corev1.Pod {
		q := resource.NewQuantity(gpus, resource.DecimalSI)
		dc := corev1.ResourceName(resourcev1.ResourceDeviceClassPrefix + nodeinfo.DraDriverGpuNvidia)
		return &corev1.Pod{
			// Same external job id: these are members of one Slurm gang job.
			ObjectMeta: metav1.ObjectMeta{
				Namespace: metav1.NamespaceDefault,
				Name:      name,
				Labels:    map[string]string{"slinky.slurm.net/slurm-jobid": "42"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "main",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{dc: *q},
						Limits:   corev1.ResourceList{dc: *q},
					},
				}},
			},
		}
	}

	// Slurm reports the whole node (8 GPUs) for the gang job on each node.
	wholeNode := func(node string) *slurmcontrol.NodeResources {
		return &slurmcontrol.NodeResources{
			Node: node,
			Gres: []slurmcontrol.GresLayout{{Name: "gpu", Type: "h100", Count: 8, Index: "0-7"}},
		}
	}

	sb := &SlurmBridge{
		Client:     kclient,
		gpuTypeMap: map[string]string{"h100": nodeinfo.DraDriverGpuNvidia},
	}

	cases := []struct {
		pod      *corev1.Pod
		node     string
		wantCnt  int64
		wantExpr string
	}{
		{gangPod("gang-a", 2), "node1", 2, "device.attributes['gpu.nvidia.com'].name in ['gpu-0','gpu-1']"},
		{gangPod("gang-b", 3), "node2", 3, "device.attributes['gpu.nvidia.com'].name in ['gpu-0','gpu-1','gpu-2']"},
	}
	for _, c := range cases {
		claim, mappings, err := sb.createRequestsAndMappings(ctx, c.pod, c.node, wholeNode(c.node))
		if err != nil {
			t.Fatalf("%s: createRequestsAndMappings() error = %v", c.pod.Name, err)
		}
		if claim == nil || len(claim.Spec.Devices.Requests) != 1 {
			t.Fatalf("%s: want exactly one device request, got claim=%v", c.pod.Name, claim)
		}
		req := claim.Spec.Devices.Requests[0]
		if req.Exactly == nil || req.Exactly.Count != c.wantCnt {
			t.Errorf("%s: device request Count = %v, want %d (pod's own request, not the node's 8)", c.pod.Name, req.Exactly, c.wantCnt)
		}
		if len(req.Exactly.Selectors) != 1 || req.Exactly.Selectors[0].CEL == nil ||
			req.Exactly.Selectors[0].CEL.Expression != c.wantExpr {
			t.Errorf("%s: selector = %+v, want CEL %q", c.pod.Name, req.Exactly.Selectors, c.wantExpr)
		}
		if len(mappings) != 1 || mappings[0].RequestName != "gpu" {
			t.Errorf("%s: mappings = %+v, want one mapping with RequestName=gpu", c.pod.Name, mappings)
		}
	}
}

func TestSlurmBridge_manageResourceClaim_deletesClaimOnError(t *testing.T) {
	ctx := context.Background()
	injectedErr := errors.New("injected client error")

	newPod := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: metav1.NamespaceDefault,
				Name:      "foo",
				UID:       "123",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "foo",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("1"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("1"),
							},
						},
					},
				},
			},
		}
	}

	resources := &slurmcontrol.NodeResources{
		Node: "node1",
		Gres: []slurmcontrol.GresLayout{
			{
				Name:  "gpu",
				Type:  nodeinfo.DraExampleDriver,
				Count: 1,
				Index: "0",
			},
		},
	}
	newClient := func(pod *corev1.Pod, funcs interceptor.Funcs) client.Client {
		return fake.NewClientBuilder().
			WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
			WithObjects(
				pod,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
					},
				},
				&resourcev1.DeviceClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeinfo.DraExampleDriver,
					},
				},
				&resourcev1.ResourceSlice{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1-gpu",
					},
					Spec: resourcev1.ResourceSliceSpec{
						NodeName: ptr.To("node1"),
						Driver:   nodeinfo.DraExampleDriver,
						Devices: []resourcev1.Device{
							{
								Name: "gpu-0",
								Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
									nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](0)},
								},
							},
						},
					},
				},
			).
			WithStatusSubresource(
				pod,
				&resourcev1.ResourceClaim{},
			).
			WithInterceptorFuncs(funcs).
			Build()
	}

	tests := []struct {
		name  string
		funcs interceptor.Funcs
	}{
		{
			name: "create claim failure",
			funcs: interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*resourcev1.ResourceClaim); ok {
						if err := c.Create(ctx, obj, opts...); err != nil {
							return err
						}
						return injectedErr
					}
					return c.Create(ctx, obj, opts...)
				},
			},
		},
		{
			name: "bind claim failure",
			funcs: interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if subResourceName == "status" {
						if _, ok := obj.(*resourcev1.ResourceClaim); ok {
							return injectedErr
						}
					}
					return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
				},
			},
		},
		{
			name: "pod status failure",
			funcs: interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if subResourceName == "status" {
						if _, ok := obj.(*corev1.Pod); ok {
							return injectedErr
						}
					}
					return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := newPod()
			kclient := newClient(pod, tt.funcs)
			sb := &SlurmBridge{
				Client: kclient,
			}

			gotErr := sb.manageResourceClaim(ctx, pod, resources.Node, resources)
			if gotErr == nil {
				t.Fatal("SlurmBridge.manageResourceClaim() error = nil, want error")
			}

			claimList := &resourcev1.ResourceClaimList{}
			if err := kclient.List(ctx, claimList); err != nil {
				t.Fatalf("Client.List(ResourceClaimList) error = %v, want nil", err)
			}
			if len(claimList.Items) != 0 {
				t.Fatalf("Client.List(ResourceClaimList) got %d claims, want 0", len(claimList.Items))
			}
		})
	}
}

func TestSlurmBridge_bindClaim(t *testing.T) {
	tests := []struct {
		name      string
		kclient   client.Client
		claim     *resourcev1.ResourceClaim
		pod       *corev1.Pod
		nodeName  string
		resources *slurmcontrol.NodeResources
		wantErr   bool
	}{
		{
			name: "smoke",
			kclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeinfo.DraDriverCpu,
						},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1-cpu",
						},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node1"),
							Driver:   nodeinfo.DraDriverCpu,
							Devices: []resourcev1.Device{
								{
									Name: "cpu0",
									Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
										nodeinfo.DraDriverCpu_CpuID:    {IntValue: ptr.To[int64](0)},
										nodeinfo.DraDriverCpu_CoreID:   {IntValue: ptr.To[int64](0)},
										nodeinfo.DraDriverCpu_SocketID: {IntValue: ptr.To[int64](0)},
										nodeinfo.DraDriverCpu_CoreType: {IntValue: ptr.To(int64(nodeinfo.CoreTypeStandard))},
									},
								},
								{
									Name: "cpu1",
									Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
										nodeinfo.DraDriverCpu_CpuID:    {IntValue: ptr.To[int64](1)},
										nodeinfo.DraDriverCpu_CoreID:   {IntValue: ptr.To[int64](0)},
										nodeinfo.DraDriverCpu_SocketID: {IntValue: ptr.To[int64](0)},
										nodeinfo.DraDriverCpu_CoreType: {IntValue: ptr.To(int64(nodeinfo.CoreTypeStandard))},
									},
								},
								{
									Name: "cpu2",
									Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
										nodeinfo.DraDriverCpu_CpuID:    {IntValue: ptr.To[int64](2)},
										nodeinfo.DraDriverCpu_CoreID:   {IntValue: ptr.To[int64](1)},
										nodeinfo.DraDriverCpu_SocketID: {IntValue: ptr.To[int64](0)},
										nodeinfo.DraDriverCpu_CoreType: {IntValue: ptr.To(int64(nodeinfo.CoreTypeStandard))},
									},
								},
								{
									Name: "cpu3",
									Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
										nodeinfo.DraDriverCpu_CpuID:    {IntValue: ptr.To[int64](3)},
										nodeinfo.DraDriverCpu_CoreID:   {IntValue: ptr.To[int64](1)},
										nodeinfo.DraDriverCpu_SocketID: {IntValue: ptr.To[int64](0)},
										nodeinfo.DraDriverCpu_CoreType: {IntValue: ptr.To(int64(nodeinfo.CoreTypeStandard))},
									},
								},
							},
						},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeinfo.DraExampleDriver,
						},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1-gpu",
						},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node1"),
							Driver:   nodeinfo.DraExampleDriver,
							Devices: []resourcev1.Device{
								{
									Name: "gpu-0",
									Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
										nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](0)},
									},
								},
								{
									Name: "gpu-1",
									Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
										nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](1)},
									},
								},
								{
									Name: "gpu-2",
									Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
										nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](2)},
									},
								},
								{
									Name: "gpu-3",
									Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
										nodeinfo.DraExampleDriver_Index: {IntValue: ptr.To[int64](3)},
									},
								},
							},
						},
					},
					&resourcev1.ResourceClaim{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: metav1.NamespaceDefault,
							Name:      "claim1",
						},
						Spec: resourcev1.ResourceClaimSpec{
							Devices: resourcev1.DeviceClaim{
								Requests: []resourcev1.DeviceRequest{
									{
										Name: "cpu",
										Exactly: &resourcev1.ExactDeviceRequest{
											DeviceClassName: nodeinfo.DraDriverCpu,
											Count:           4,
											Selectors: []resourcev1.DeviceSelector{
												{
													CEL: &resourcev1.CELDeviceSelector{
														Expression: "device.attributes['dra.cpu'].cpuID in [0,1,2,3]",
													},
												},
											},
										},
									},
									{
										Name: "gpu",
										Exactly: &resourcev1.ExactDeviceRequest{
											DeviceClassName: nodeinfo.DraExampleDriver,
											Count:           3,
											Selectors: []resourcev1.DeviceSelector{
												{
													CEL: &resourcev1.CELDeviceSelector{
														Expression: "device.attributes['gpu.example.com'].index in [1,3,4]",
													},
												},
											},
										},
									},
								},
							},
						},
					},
					&corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: metav1.NamespaceDefault,
							Name:      "foo",
							UID:       "123",
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "foo",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("4"),
											corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("4"),
											corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
										},
									},
								},
							},
						},
					},
				).
				WithStatusSubresource(
					&resourcev1.ResourceClaim{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: metav1.NamespaceDefault,
							Name:      "claim1",
						},
						Spec: resourcev1.ResourceClaimSpec{
							Devices: resourcev1.DeviceClaim{
								Requests: []resourcev1.DeviceRequest{
									{
										Name: "gpu",
										Exactly: &resourcev1.ExactDeviceRequest{
											DeviceClassName: nodeinfo.DraExampleDriver,
											Count:           3,
											Selectors: []resourcev1.DeviceSelector{
												{
													CEL: &resourcev1.CELDeviceSelector{
														Expression: "device.attributes['gpu.example.com'].index in [1,3,4]",
													},
												},
											},
										},
									},
								},
							},
						},
					},
					&corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: metav1.NamespaceDefault,
							Name:      "foo",
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "foo",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("4"),
											corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("4"),
											corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
										},
									},
								},
							},
						},
					},
				).
				Build(),
			claim: &resourcev1.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: metav1.NamespaceDefault,
					Name:      "claim1",
				},
				Spec: resourcev1.ResourceClaimSpec{
					Devices: resourcev1.DeviceClaim{
						Requests: []resourcev1.DeviceRequest{
							{
								Name: "cpu",
								Exactly: &resourcev1.ExactDeviceRequest{
									DeviceClassName: nodeinfo.DraDriverCpu,
									Count:           4,
									Selectors: []resourcev1.DeviceSelector{
										{
											CEL: &resourcev1.CELDeviceSelector{
												Expression: "device.attributes['dra.cpu'].cpuID in [0,1,2,3]",
											},
										},
									},
								},
							},
							{
								Name: "gpu",
								Exactly: &resourcev1.ExactDeviceRequest{
									DeviceClassName: nodeinfo.DraExampleDriver,
									Count:           3,
									Selectors: []resourcev1.DeviceSelector{
										{
											CEL: &resourcev1.CELDeviceSelector{
												Expression: "device.attributes['gpu.example.com'].index in [1,3,4]",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: metav1.NamespaceDefault,
					Name:      "foo",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "foo",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("4"),
									corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("4"),
									corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu.example.com"): resource.MustParse("3"),
								},
							},
						},
					},
				},
			},
			nodeName: "node1",
			resources: &slurmcontrol.NodeResources{
				Node: "node1",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  "example.com",
						Count: 3,
						Index: "0,2-3",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := &SlurmBridge{
				Client: tt.kclient,
			}
			gotErr := sb.bindClaim(context.Background(), tt.claim, tt.pod, tt.nodeName, tt.resources)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("bindClaim() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("bindClaim() succeeded unexpectedly")
			}
		})
	}
}

func TestSlurmBridge_patchPodExtendedResourceClaimStatus(t *testing.T) {
	tests := []struct {
		name            string
		kclient         client.Client
		pod             *corev1.Pod
		claim           *resourcev1.ResourceClaim
		requestMappings []corev1.ContainerExtendedResourceRequest
		wantErr         bool
	}{
		{
			name:    "empty",
			kclient: fake.NewFakeClient(),
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: corev1.NamespaceDefault,
					Name:      "foo",
				},
			},
			wantErr: true,
		},
		{
			name: "smoke",
			kclient: fake.NewClientBuilder().
				WithObjects(
					&corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: corev1.NamespaceDefault,
							Name:      "foo",
						},
					},
				).
				WithStatusSubresource(
					&corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: corev1.NamespaceDefault,
							Name:      "foo",
						},
					},
				).
				Build(),
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: corev1.NamespaceDefault,
					Name:      "foo",
				},
			},
			claim: &resourcev1.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: metav1.NamespaceDefault,
					Name:      "claim1",
				},
				Spec: resourcev1.ResourceClaimSpec{
					Devices: resourcev1.DeviceClaim{
						Requests: []resourcev1.DeviceRequest{
							{
								Name: "gpu",
								Exactly: &resourcev1.ExactDeviceRequest{
									DeviceClassName: nodeinfo.DraExampleDriver,
									Count:           3,
									Selectors: []resourcev1.DeviceSelector{
										{
											CEL: &resourcev1.CELDeviceSelector{
												Expression: "device.attributes['gpu.example.com'].index in [1,3,4]",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			requestMappings: []corev1.ContainerExtendedResourceRequest{
				{
					ContainerName: "foo",
					ResourceName:  "cpu",
					RequestName:   "container-0-request-0",
				},
				{
					ContainerName: "foo",
					ResourceName:  "deviceclass.resource.kubernetes.io/gpu.example.com",
					RequestName:   "container-0-request-1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := &SlurmBridge{
				Client: tt.kclient,
			}
			gotErr := sb.patchPodExtendedResourceClaimStatus(context.Background(), tt.pod, tt.claim, tt.requestMappings)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("patchPodExtendedResourceClaimStatus() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("patchPodExtendedResourceClaimStatus() succeeded unexpectedly")
			}
		})
	}
}
