// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package nodeinfo

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/puttsk/hostlist"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/SlinkyProject/slurm-bridge/internal/scheduler/plugins/slurmbridge/slurmcontrol"
	"github.com/SlinkyProject/slurm-bridge/internal/utils/bitmaputil"
)

// Represents a Kubernetes node for Slurm.
type NodeInfo struct {
	CpuMap CPUMap
	GpuMap GPUMap
}

// ResolveDeviceClass maps a Slurm GPU GRES type name to a Kubernetes DRA
// DeviceClass name using the configured gpuTypeMap.
//
// With AutoDetect=nvidia, Slurm names GPU GRES by model (e.g. "nvidia_b200"),
// which does not match the DRA DeviceClass name ("gpu.nvidia.com"). gpuTypeMap
// lets operators declare that a Slurm GRES type should be treated as a given
// DeviceClass. When no (non-empty) mapping is configured for slurmType, it is
// returned unchanged, preserving the default assumption that the Slurm GRES type
// name equals the DeviceClass name.
func ResolveDeviceClass(gpuTypeMap map[string]string, slurmType string) string {
	if deviceClass, ok := gpuTypeMap[slurmType]; ok && deviceClass != "" {
		return deviceClass
	}
	return slurmType
}

// PodDeviceClassRequest returns the number of devices the pod itself requests
// for the given DRA DeviceClass, considering both the DRA extended-resource
// form ("deviceclass.resource.kubernetes.io/<class>") and the legacy device-
// plugin form ("nvidia.com/gpu", "amd.com/gpu").
//
// The count uses the SAME pod-level aggregation the forward path applies to
// derive the Slurm gres request: slurmjobir.parseGPUDevicePlugin also calls
// resourcehelper.PodLimits, which sums GPU requests across regular containers
// (and maxes them with init containers). This keeps the clamp count equal to
// the gres/gpu=N Slurm allocated; a naive per-container max would undercount a
// pod that splits its GPUs across multiple containers.
//
// This is the pod's *intent*. It is needed to clamp the per-pod ResourceClaim:
// when Slurm allocates a node exclusively (whole-node), its NodeResourceLayout
// reports the node's entire GPU pool for the job, not the slice this pod meant
// to use. Without clamping, a pod that asked for 1 GPU would be handed all of
// the node's GPUs. Returns (0, false) when the pod requests nothing for the
// DeviceClass.
func PodDeviceClassRequest(pod *corev1.Pod, deviceClassName string) (int64, bool) {
	if pod == nil || deviceClassName == "" {
		return 0, false
	}
	draResourceName := resourcev1.ResourceDeviceClassPrefix + deviceClassName

	lim := resourcehelper.PodLimits(pod, resourcehelper.PodResourcesOptions{})
	var max int64
	found := false
	for rName, quantity := range lim {
		name := rName.String()
		isDevicePlugin := name == DevicePluginNvidia || name == DevicePluginAmd
		isThisDeviceClass := name == draResourceName
		if !isDevicePlugin && !isThisDeviceClass {
			continue
		}
		if v := quantity.Value(); v > max {
			max = v
		}
		found = true
	}
	return max, found
}

// clampDeviceList trims the Slurm-reported device indices and count to what the
// pod actually requested for the DeviceClass. When the pod requests fewer
// devices than Slurm reports allocated (the whole-node/exclusive case), only
// the first podRequest indices are kept so the generated ResourceClaim pins to
// the right number of devices. When the pod requests at least as many as Slurm
// reports (the normal shared case), the Slurm-reported list is returned
// unchanged.
func clampDeviceList(indexList []string, podRequest int64, podHasRequest bool) []string {
	if !podHasRequest || podRequest <= 0 {
		return indexList
	}
	if int64(len(indexList)) <= podRequest {
		return indexList
	}
	return indexList[:podRequest]
}

func (n *NodeInfo) GetDeviceRequests(ctx context.Context, kubeclient client.Client, pod *corev1.Pod, resources *slurmcontrol.NodeResources, gpuTypeMap map[string]string) ([]resourcev1.DeviceRequest, error) {
	var requests []resourcev1.DeviceRequest

	if resources == nil {
		return requests, nil
	}

	if hasDeviceClass(ctx, kubeclient, DraDriverCpu) {
		bitmap, err := bitmaputil.NewFrom(resources.CoreBitmap)
		if err != nil {
			return nil, err
		}
		cpuSet := n.CpuMap.ToMachineCPUs(bitmap)
		cpuSetString := strings.ReplaceAll(fmt.Sprint(cpuSet.List()), " ", ",")
		req := resourcev1.DeviceRequest{
			Name: corev1.ResourceCPU.String(),
			Exactly: &resourcev1.ExactDeviceRequest{
				DeviceClassName: DraDriverCpu,
				AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
				Count:           int64(cpuSet.Size()),
				Selectors: []resourcev1.DeviceSelector{
					{
						CEL: &resourcev1.CELDeviceSelector{
							Expression: fmt.Sprintf("device.attributes['%s'].cpuID in %s", DraDriverCpu, cpuSetString),
						},
					},
				},
			},
		}
		requests = append(requests, req)
	}

	for _, gres := range resources.Gres {
		deviceClassName := ResolveDeviceClass(gpuTypeMap, gres.Type)
		if !hasDeviceClass(ctx, kubeclient, deviceClassName) {
			continue
		}
		indexList, err := hostlist.Expand(fmt.Sprintf("[%s]", gres.Index))
		if err != nil {
			return nil, err
		}
		// Clamp the Slurm-reported device list to what this pod actually
		// requested. On a whole-node/exclusive Slurm allocation, the layout
		// reports the node's entire GPU pool, which would otherwise pin the
		// claim to every device on the node (and request that many).
		podRequest, podHasRequest := PodDeviceClassRequest(pod, deviceClassName)
		indexList = clampDeviceList(indexList, podRequest, podHasRequest)
		count := int64(len(indexList))
		var celExpr string
		switch deviceClassName {
		case DraDriverGpuNvidia:
			// NVIDIA k8s-dra-driver-gpu: use device.attributes['gpu.nvidia.com'].name (e.g. "gpu-0", "gpu-1").
			names := make([]string, 0, len(indexList))
			for _, i := range indexList {
				names = append(names, fmt.Sprintf("'gpu-%s'", i))
			}
			celExpr = fmt.Sprintf("device.attributes['%s'].name in [%s]", DraDriverGpuNvidia, strings.Join(names, ","))
		case DraExampleDriver:
			// Example DRA driver: use device.attributes['gpu.example.com'].index (e.g. 0, 1, 2).
			indexListString := strings.Join(indexList, ",")
			celExpr = fmt.Sprintf("device.attributes['%s'].index in [%s]", deviceClassName, indexListString)
		default:
			continue
		}
		req := resourcev1.DeviceRequest{
			Name: gres.Name,
			Exactly: &resourcev1.ExactDeviceRequest{
				DeviceClassName: deviceClassName,
				AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
				Count:           count,
				Selectors: []resourcev1.DeviceSelector{
					{
						CEL: &resourcev1.CELDeviceSelector{
							Expression: celExpr,
						},
					},
				},
			},
		}
		requests = append(requests, req)
	}

	return requests, nil
}

func (n *NodeInfo) GetDeviceRequestAllocationResult(ctx context.Context, kubeclient client.Client, pod *corev1.Pod, resources *slurmcontrol.NodeResources, gpuTypeMap map[string]string) ([]resourcev1.DeviceRequestAllocationResult, error) {
	var devices []resourcev1.DeviceRequestAllocationResult

	if resources == nil {
		return devices, nil
	}

	if hasDeviceClass(ctx, kubeclient, DraDriverCpu) {
		bitmap, err := bitmaputil.NewFrom(resources.CoreBitmap)
		if err != nil {
			return nil, err
		}
		// Individual Mode: each CPU is enumerated
		cpuSet := n.CpuMap.ToMachineCPUs(bitmap)
		for _, cpuID := range cpuSet.List() {
			cpuInfo, ok := n.CpuMap.CPUInfoMap[cpuID]
			if !ok {
				continue
			}
			dev := resourcev1.DeviceRequestAllocationResult{
				Request: corev1.ResourceCPU.String(),
				Driver:  DraDriverCpu,
				Pool:    resources.Node,
				Device:  cpuInfo.Name,
			}
			devices = append(devices, dev)
		}
	}

	for _, gres := range resources.Gres {
		deviceClassName := ResolveDeviceClass(gpuTypeMap, gres.Type)
		if !hasDeviceClass(ctx, kubeclient, deviceClassName) {
			continue
		}
		indexList, err := hostlist.Expand(fmt.Sprintf("[%s]", gres.Index))
		if err != nil {
			return nil, err
		}
		// Clamp to the pod's own request so the allocation result enumerates
		// exactly the devices the (clamped) DeviceRequest pins to. See
		// GetDeviceRequests for the whole-node/exclusive rationale.
		podRequest, podHasRequest := PodDeviceClassRequest(pod, deviceClassName)
		indexList = clampDeviceList(indexList, podRequest, podHasRequest)
		for _, i := range indexList {
			index, err := strconv.Atoi(i)
			if err != nil {
				return nil, err
			}
			gpuInfo, ok := n.GpuMap.GPUInfoMap[index]
			if !ok {
				continue
			}
			dev := resourcev1.DeviceRequestAllocationResult{
				Request: gres.Name,
				Driver:  deviceClassName,
				Pool:    resources.Node,
				Device:  gpuInfo.Name,
			}
			devices = append(devices, dev)
		}
	}

	return devices, nil
}

func NewNodeInfo(ctx context.Context, kubeclient client.Client, nodeName string) (*NodeInfo, error) {
	resourceSliceList := &resourcev1.ResourceSliceList{}
	if err := kubeclient.List(ctx, resourceSliceList); err != nil {
		return nil, err
	}

	nodeInfo := &NodeInfo{}
	for _, resourceSlice := range resourceSliceList.Items {
		if ptr.Deref(resourceSlice.Spec.NodeName, "") != nodeName {
			continue
		}
		switch resourceSlice.Spec.Driver {
		case DraDriverCpu:
			cpuInfos := NewCPUInfos(&resourceSlice)
			nodeInfo.CpuMap = NewCPUMap(cpuInfos)
		case DraExampleDriver, DraDriverGpuNvidia:
			gpuInfos := NewGPUInfos(ctx, &resourceSlice)
			nodeInfo.GpuMap = NewGPUMap(resourceSlice.Spec.Driver, gpuInfos)
		default:
			// TODO: can we even default?
		}
	}

	return nodeInfo, nil
}

// GetGresAndGresConf returns Slurm GRES and GresConf strings for this node's devices.
// GRES and GresConf are derived from DRA ResourceSlices (e.g. GPU devices); CPU is not included.
// Returns ("", "") when the node has no GRES devices.
func (n *NodeInfo) GetGresAndGresConf() (gres, gresConf string) {
	if len(n.GpuMap.GPUInfoMap) == 0 {
		return "", ""
	}
	// Build gres: "gpu:driver:count"
	count := len(n.GpuMap.GPUInfoMap)
	gres = fmt.Sprintf("gpu:%s:%d", n.GpuMap.Driver, count)

	// Build gresConf: count=N,name=gpu,type=driver,file=name0,file=name1,...
	// Slurm requires count= and one file= per device for create node to succeed.
	indices := make([]int, 0, count)
	for idx := range n.GpuMap.GPUInfoMap {
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	fileParts := make([]string, 0, count)
	for _, idx := range indices {
		info := n.GpuMap.GPUInfoMap[idx]
		deviceName := fmt.Sprintf("gpu-%d", idx)
		if info != nil && info.Name != "" {
			deviceName = info.Name
		}
		fileParts = append(fileParts, "file="+deviceName)
	}
	gresConf = fmt.Sprintf("count=%d,name=gpu,type=%s,%s", count, n.GpuMap.Driver, strings.Join(fileParts, ","))
	return gres, gresConf
}

func hasDeviceClass(ctx context.Context, kubeclient client.Client, deviceClassName string) bool {
	if deviceClassName == "" {
		return false
	}
	deviceClass := &resourcev1.DeviceClass{}
	err := kubeclient.Get(ctx, types.NamespacedName{Name: deviceClassName}, deviceClass)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		return false
	}
	return true
}
