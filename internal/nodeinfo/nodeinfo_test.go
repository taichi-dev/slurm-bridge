// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package nodeinfo_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/SlinkyProject/slurm-bridge/internal/nodeinfo"
	"github.com/SlinkyProject/slurm-bridge/internal/scheduler/plugins/slurmbridge/slurmcontrol"
	"github.com/SlinkyProject/slurm-bridge/internal/utils/bitmaputil"
)

func init() {
	utilruntime.Must(scheme.AddToScheme(scheme.Scheme))
	utilruntime.Must(resourcev1.AddToScheme(scheme.Scheme))
}

// newPodWithDeviceClassRequest builds a Pod whose single container requests
// `count` of the given DRA DeviceClass via the extended-resource form
// ("deviceclass.resource.kubernetes.io/<class>").
func newPodWithDeviceClassRequest(deviceClassName string, count int64) *corev1.Pod {
	rName := corev1.ResourceName(resourcev1.ResourceDeviceClassPrefix + deviceClassName)
	q := resource.NewQuantity(count, resource.DecimalSI)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "pod"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "main",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{rName: *q},
						Limits:   corev1.ResourceList{rName: *q},
					},
				},
			},
		},
	}
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

func TestNodeInfo_GetDeviceRequests(t *testing.T) {
	tests := []struct {
		name       string
		kubeclient client.Client
		nodeName   string
		pod        *corev1.Pod
		resources  *slurmcontrol.NodeResources
		gpuTypeMap map[string]string
		want       []resourcev1.DeviceRequest
		wantErr    bool
	}{
		{
			name: "dra.cpu",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverCpu},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
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
				).
				Build(),
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node:       "node",
				CoreBitmap: bitmaputil.String(bitmaputil.New(0)),
			},
			want: []resourcev1.DeviceRequest{
				{
					Name: "cpu",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: nodeinfo.DraDriverCpu,
						AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
						Count:           2,
						Selectors: []resourcev1.DeviceSelector{
							{
								CEL: &resourcev1.CELDeviceSelector{
									Expression: "device.attributes['dra.cpu'].cpuID in [0,1]",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "gpu.example.com",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraExampleDriver},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
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
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  nodeinfo.DraExampleDriver,
						Count: 2,
						Index: "0-1",
					},
				},
			},
			want: []resourcev1.DeviceRequest{
				{
					Name: "gpu",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: nodeinfo.DraExampleDriver,
						AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
						Count:           2,
						Selectors: []resourcev1.DeviceSelector{
							{
								CEL: &resourcev1.CELDeviceSelector{
									Expression: "device.attributes['gpu.example.com'].index in [0,1]",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "gpu.nvidia.com",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia},
					},
					// NVIDIA k8s-dra-driver-gpu uses device Name "gpu-<minor>" (no "index" attribute).
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   nodeinfo.DraDriverGpuNvidia,
							Devices: []resourcev1.Device{
								{Name: "gpu-0"},
								{Name: "gpu-1"},
							},
						},
					},
				).
				Build(),
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  nodeinfo.DraDriverGpuNvidia,
						Count: 2,
						Index: "0-1",
					},
				},
			},
			want: []resourcev1.DeviceRequest{
				{
					Name: "gpu",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: nodeinfo.DraDriverGpuNvidia,
						AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
						Count:           2,
						Selectors: []resourcev1.DeviceSelector{
							{
								CEL: &resourcev1.CELDeviceSelector{
									Expression: "device.attributes['gpu.nvidia.com'].name in ['gpu-0','gpu-1']",
								},
							},
						},
					},
				},
			},
		},
		{
			// AutoDetect=nvidia names the GRES type by model ("nvidia_b200"),
			// which gpuTypeMap maps to the gpu.nvidia.com DeviceClass.
			name: "gpu.nvidia.com via AutoDetect model type",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   nodeinfo.DraDriverGpuNvidia,
							Devices: []resourcev1.Device{
								{Name: "gpu-0"},
								{Name: "gpu-1"},
							},
						},
					},
				).
				Build(),
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  "nvidia_b200",
						Count: 2,
						Index: "0-1",
					},
				},
			},
			gpuTypeMap: map[string]string{"nvidia_b200": nodeinfo.DraDriverGpuNvidia},
			want: []resourcev1.DeviceRequest{
				{
					Name: "gpu",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: nodeinfo.DraDriverGpuNvidia,
						AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
						Count:           2,
						Selectors: []resourcev1.DeviceSelector{
							{
								CEL: &resourcev1.CELDeviceSelector{
									Expression: "device.attributes['gpu.nvidia.com'].name in ['gpu-0','gpu-1']",
								},
							},
						},
					},
				},
			},
		},
		{
			// Whole-node / exclusive Slurm allocation: the NodeResourceLayout
			// reports the node's ENTIRE GPU pool (count=8, indices 0-7) for the
			// job, but this pod only asked for 1 GPU. The claim must be clamped
			// to the pod's request (count=1, pinned to gpu-0) — otherwise the
			// pod is handed every GPU on the node. This is the PodGroup gang bug.
			name: "whole-node allocation clamped to pod request (gpu.nvidia.com)",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   nodeinfo.DraDriverGpuNvidia,
							Devices: []resourcev1.Device{
								{Name: "gpu-0"}, {Name: "gpu-1"}, {Name: "gpu-2"}, {Name: "gpu-3"},
								{Name: "gpu-4"}, {Name: "gpu-5"}, {Name: "gpu-6"}, {Name: "gpu-7"},
							},
						},
					},
				).
				Build(),
			nodeName:   "node",
			pod:        newPodWithDeviceClassRequest(nodeinfo.DraDriverGpuNvidia, 1),
			gpuTypeMap: map[string]string{"h100": nodeinfo.DraDriverGpuNvidia},
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  "h100",
						Count: 8,
						Index: "0-7",
					},
				},
			},
			want: []resourcev1.DeviceRequest{
				{
					Name: "gpu",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: nodeinfo.DraDriverGpuNvidia,
						AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
						Count:           1,
						Selectors: []resourcev1.DeviceSelector{
							{
								CEL: &resourcev1.CELDeviceSelector{
									Expression: "device.attributes['gpu.nvidia.com'].name in ['gpu-0']",
								},
							},
						},
					},
				},
			},
		},
		{
			// Shared (non-exclusive) allocation where Slurm already reports
			// exactly the pod's GPU. Pod request == Slurm count, so no clamping
			// occurs and the Slurm-reported device is preserved verbatim.
			name: "pod request matching slurm allocation is unchanged",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   nodeinfo.DraDriverGpuNvidia,
							Devices: []resourcev1.Device{
								{Name: "gpu-0"}, {Name: "gpu-1"},
							},
						},
					},
				).
				Build(),
			nodeName:   "node",
			pod:        newPodWithDeviceClassRequest(nodeinfo.DraDriverGpuNvidia, 2),
			gpuTypeMap: map[string]string{"h100": nodeinfo.DraDriverGpuNvidia},
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  "h100",
						Count: 2,
						Index: "0-1",
					},
				},
			},
			want: []resourcev1.DeviceRequest{
				{
					Name: "gpu",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: nodeinfo.DraDriverGpuNvidia,
						AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
						Count:           2,
						Selectors: []resourcev1.DeviceSelector{
							{
								CEL: &resourcev1.CELDeviceSelector{
									Expression: "device.attributes['gpu.nvidia.com'].name in ['gpu-0','gpu-1']",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "unknown device class name is skipped",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: "gpu.unknown.com"},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice-unknown"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   "gpu.unknown.com",
							Devices:  []resourcev1.Device{{Name: "gpu-0"}},
						},
					},
				).
				Build(),
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  "gpu.unknown.com",
						Count: 1,
						Index: "0",
					},
				},
			},
			want: nil,
		},
		{
			name: "unknown device class name is skipped when mixed with known",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraExampleDriver},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: "gpu.unknown.com"},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice-example"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
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
							},
						},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice-unknown"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   "gpu.unknown.com",
							Devices:  []resourcev1.Device{{Name: "gpu-0"}},
						},
					},
				).
				Build(),
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  nodeinfo.DraExampleDriver,
						Count: 1,
						Index: "0",
					},
					{
						Name:  "other",
						Type:  "gpu.unknown.com",
						Count: 1,
						Index: "0",
					},
				},
			},
			want: []resourcev1.DeviceRequest{
				{
					Name: "gpu",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: nodeinfo.DraExampleDriver,
						AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
						Count:           1,
						Selectors: []resourcev1.DeviceSelector{
							{
								CEL: &resourcev1.CELDeviceSelector{
									Expression: "device.attributes['gpu.example.com'].index in [0]",
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := nodeinfo.NewNodeInfo(context.Background(), tt.kubeclient, tt.nodeName)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			got, gotErr := n.GetDeviceRequests(context.Background(), tt.kubeclient, tt.pod, tt.resources, tt.gpuTypeMap)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("GetDeviceRequests() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("GetDeviceRequests() succeeded unexpectedly")
			}
			if !equality.Semantic.DeepEqual(got, tt.want) {
				t.Errorf("GetDeviceRequests() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeInfo_GetDeviceRequestAllocationResult(t *testing.T) {
	tests := []struct {
		name       string
		kubeclient client.Client
		nodeName   string
		pod        *corev1.Pod
		resources  *slurmcontrol.NodeResources
		gpuTypeMap map[string]string
		want       []resourcev1.DeviceRequestAllocationResult
		wantErr    bool
	}{
		{
			name: "dra.cpu",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverCpu},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
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
				).
				Build(),
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node:       "node",
				CoreBitmap: bitmaputil.String(bitmaputil.New(0)),
			},
			want: []resourcev1.DeviceRequestAllocationResult{
				{Request: "cpu", Driver: nodeinfo.DraDriverCpu, Device: "cpu0", Pool: "node"},
				{Request: "cpu", Driver: nodeinfo.DraDriverCpu, Device: "cpu1", Pool: "node"},
			},
		},
		{
			name: "gpu.example.com",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraExampleDriver},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
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
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  nodeinfo.DraExampleDriver,
						Count: 2,
						Index: "0-1",
					},
				},
			},
			want: []resourcev1.DeviceRequestAllocationResult{
				{Request: "gpu", Driver: nodeinfo.DraExampleDriver, Device: "gpu-0", Pool: "node"},
				{Request: "gpu", Driver: nodeinfo.DraExampleDriver, Device: "gpu-1", Pool: "node"},
			},
		},
		{
			name: "gpu.nvidia.com",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia},
					},
					// NVIDIA driver uses device Name "gpu-<minor>" (no "index" attribute).
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   nodeinfo.DraDriverGpuNvidia,
							Devices: []resourcev1.Device{
								{Name: "gpu-0"},
								{Name: "gpu-1"},
							},
						},
					},
				).
				Build(),
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  nodeinfo.DraDriverGpuNvidia,
						Count: 2,
						Index: "0-1",
					},
				},
			},
			want: []resourcev1.DeviceRequestAllocationResult{
				{Request: "gpu", Driver: nodeinfo.DraDriverGpuNvidia, Device: "gpu-0", Pool: "node"},
				{Request: "gpu", Driver: nodeinfo.DraDriverGpuNvidia, Device: "gpu-1", Pool: "node"},
			},
		},
		{
			// AutoDetect=nvidia reports the GRES type as a model name
			// ("nvidia_b200"); gpuTypeMap resolves it to the gpu.nvidia.com
			// DeviceClass, and the allocation binds the gpu-<index> devices.
			name: "gpu.nvidia.com via AutoDetect model type",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   nodeinfo.DraDriverGpuNvidia,
							Devices: []resourcev1.Device{
								{Name: "gpu-0"},
								{Name: "gpu-1"},
							},
						},
					},
				).
				Build(),
			nodeName: "node",
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  "nvidia_b200",
						Count: 2,
						Index: "0-1",
					},
				},
			},
			gpuTypeMap: map[string]string{"nvidia_b200": nodeinfo.DraDriverGpuNvidia},
			want: []resourcev1.DeviceRequestAllocationResult{
				{Request: "gpu", Driver: nodeinfo.DraDriverGpuNvidia, Device: "gpu-0", Pool: "node"},
				{Request: "gpu", Driver: nodeinfo.DraDriverGpuNvidia, Device: "gpu-1", Pool: "node"},
			},
		},
		{
			// Whole-node / exclusive Slurm allocation: the layout reports all 8
			// of the node's GPUs, but the pod asked for 1. The allocation result
			// must enumerate exactly the clamped device set (gpu-0 only), so it
			// matches the (also-clamped) DeviceRequest. This is the PodGroup
			// gang bug on the allocation-result side.
			name: "whole-node allocation clamped to pod request (gpu.nvidia.com)",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{Name: "node"},
					},
					&resourcev1.DeviceClass{
						ObjectMeta: metav1.ObjectMeta{Name: nodeinfo.DraDriverGpuNvidia},
					},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-slice"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
							Driver:   nodeinfo.DraDriverGpuNvidia,
							Devices: []resourcev1.Device{
								{Name: "gpu-0"}, {Name: "gpu-1"}, {Name: "gpu-2"}, {Name: "gpu-3"},
								{Name: "gpu-4"}, {Name: "gpu-5"}, {Name: "gpu-6"}, {Name: "gpu-7"},
							},
						},
					},
				).
				Build(),
			nodeName:   "node",
			pod:        newPodWithDeviceClassRequest(nodeinfo.DraDriverGpuNvidia, 1),
			gpuTypeMap: map[string]string{"h100": nodeinfo.DraDriverGpuNvidia},
			resources: &slurmcontrol.NodeResources{
				Node: "node",
				Gres: []slurmcontrol.GresLayout{
					{
						Name:  "gpu",
						Type:  "h100",
						Count: 8,
						Index: "0-7",
					},
				},
			},
			want: []resourcev1.DeviceRequestAllocationResult{
				{Request: "gpu", Driver: nodeinfo.DraDriverGpuNvidia, Device: "gpu-0", Pool: "node"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := nodeinfo.NewNodeInfo(context.Background(), tt.kubeclient, tt.nodeName)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			got, gotErr := n.GetDeviceRequestAllocationResult(context.Background(), tt.kubeclient, tt.pod, tt.resources, tt.gpuTypeMap)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("GetDeviceRequestAllocationResult() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("GetDeviceRequestAllocationResult() succeeded unexpectedly")
			}
			if !equality.Semantic.DeepEqual(got, tt.want) {
				t.Errorf("GetDeviceRequestAllocationResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeInfo_GetGresAndGresConf(t *testing.T) {
	tests := []struct {
		name       string
		kubeclient client.Client
		nodeName   string
		wantGres   string
		wantConf   string
	}{
		{
			name: "no GRES when node has no GPU ResourceSlice",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}},
				).
				Build(),
			nodeName: "node",
			wantGres: "",
			wantConf: "",
		},
		{
			name: "no GRES when node has only CPU ResourceSlice",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-cpu"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
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
							},
						},
					},
				).
				Build(),
			nodeName: "node",
			wantGres: "",
			wantConf: "",
		},
		{
			name: "example driver with single GPU",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-gpu"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
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
				Build(),
			nodeName: "node",
			wantGres: "gpu:gpu.example.com:1",
			wantConf: "count=1,name=gpu,type=gpu.example.com,file=gpu-0",
		},
		{
			name: "example driver with four GPUs",
			kubeclient: fake.NewClientBuilder().
				WithIndex(&resourcev1.ResourceSlice{}, "spec.nodeName", resourceSliceNodeIndex).
				WithObjects(
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}},
					&resourcev1.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{Name: "node-gpu"},
						Spec: resourcev1.ResourceSliceSpec{
							NodeName: ptr.To("node"),
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
			nodeName: "node",
			wantGres: "gpu:gpu.example.com:4",
			wantConf: "count=4,name=gpu,type=gpu.example.com,file=gpu-0,file=gpu-1,file=gpu-2,file=gpu-3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := nodeinfo.NewNodeInfo(context.Background(), tt.kubeclient, tt.nodeName)
			if err != nil {
				t.Fatalf("NewNodeInfo() failed: %v", err)
			}
			gotGres, gotConf := n.GetGresAndGresConf()
			if gotGres != tt.wantGres {
				t.Errorf("GetGresAndGresConf() gres = %q, want %q", gotGres, tt.wantGres)
			}
			if gotConf != tt.wantConf {
				t.Errorf("GetGresAndGresConf() gresConf = %q, want %q", gotConf, tt.wantConf)
			}
		})
	}
}

func TestResolveDeviceClass(t *testing.T) {
	tests := []struct {
		name       string
		gpuTypeMap map[string]string
		slurmType  string
		want       string
	}{
		{
			name:      "nil map returns slurm type unchanged",
			slurmType: "gpu.nvidia.com",
			want:      "gpu.nvidia.com",
		},
		{
			name:       "mapped model type resolves to device class",
			gpuTypeMap: map[string]string{"nvidia_b200": "gpu.nvidia.com"},
			slurmType:  "nvidia_b200",
			want:       "gpu.nvidia.com",
		},
		{
			name:       "unmapped type returns unchanged",
			gpuTypeMap: map[string]string{"nvidia_b200": "gpu.nvidia.com"},
			slurmType:  "gpu.example.com",
			want:       "gpu.example.com",
		},
		{
			name:       "empty mapping value is ignored",
			gpuTypeMap: map[string]string{"nvidia_b200": ""},
			slurmType:  "nvidia_b200",
			want:       "nvidia_b200",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nodeinfo.ResolveDeviceClass(tt.gpuTypeMap, tt.slurmType); got != tt.want {
				t.Errorf("ResolveDeviceClass() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPodDeviceClassRequest(t *testing.T) {
	withDevicePlugin := func(name string, count int64) *corev1.Pod {
		q := resource.NewQuantity(count, resource.DecimalSI)
		return &corev1.Pod{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "main",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceName(name): *q},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name            string
		pod             *corev1.Pod
		deviceClassName string
		wantCount       int64
		wantFound       bool
	}{
		{
			name:            "nil pod",
			pod:             nil,
			deviceClassName: nodeinfo.DraDriverGpuNvidia,
			wantFound:       false,
		},
		{
			name:            "DRA extended resource request",
			pod:             newPodWithDeviceClassRequest(nodeinfo.DraDriverGpuNvidia, 1),
			deviceClassName: nodeinfo.DraDriverGpuNvidia,
			wantCount:       1,
			wantFound:       true,
		},
		{
			name:            "DRA extended resource request for a different class is ignored",
			pod:             newPodWithDeviceClassRequest(nodeinfo.DraExampleDriver, 4),
			deviceClassName: nodeinfo.DraDriverGpuNvidia,
			wantFound:       false,
		},
		{
			name:            "nvidia device-plugin request maps to the resolved device class",
			pod:             withDevicePlugin(nodeinfo.DevicePluginNvidia, 2),
			deviceClassName: nodeinfo.DraDriverGpuNvidia,
			wantCount:       2,
			wantFound:       true,
		},
		{
			name:            "no gpu request",
			pod:             &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}}},
			deviceClassName: nodeinfo.DraDriverGpuNvidia,
			wantFound:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCount, gotFound := nodeinfo.PodDeviceClassRequest(tt.pod, tt.deviceClassName)
			if gotFound != tt.wantFound {
				t.Errorf("PodDeviceClassRequest() found = %v, want %v", gotFound, tt.wantFound)
			}
			if gotCount != tt.wantCount {
				t.Errorf("PodDeviceClassRequest() count = %d, want %d", gotCount, tt.wantCount)
			}
		})
	}
}
