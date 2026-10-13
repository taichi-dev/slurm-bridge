// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmjobir

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

func TestResourceRequestsIncludeSidecarsInitAndOverhead(t *testing.T) {
	pod := podWithResources("4", "48Gi", "6", "96Gi")
	pod.Spec.InitContainers = []corev1.Container{{
		Name: "store", RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways),
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m"), corev1.ResourceMemory: resource.MustParse("18Gi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("20Gi")},
		},
	}}
	pod.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("1Gi")}
	for _, tc := range []struct {
		name   string
		init   bool
		cpu    int32
		memory int64
	}{
		{name: "restartable sidecar and overhead", cpu: 5, memory: 67 * 1024},
		{name: "init peak dominates steady state", init: true, cpu: 7, memory: 81 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := pod.DeepCopy()
			if tc.init {
				p.Spec.InitContainers = append(p.Spec.InitContainers, corev1.Container{
					Name: "prepare", Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("62Gi")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20"), corev1.ResourceMemory: resource.MustParse("120Gi")},
					},
				})
			}
			ir := &SlurmJobIR{Pods: corev1.PodList{Items: []corev1.Pod{*p}}}
			parsePodsCpuAndMemory(ir)
			if ir.JobInfo.CpuPerTask == nil || *ir.JobInfo.CpuPerTask != tc.cpu {
				t.Fatalf("CPU reservation = %v, want %d", ir.JobInfo.CpuPerTask, tc.cpu)
			}
			if ir.JobInfo.MemPerNode == nil || *ir.JobInfo.MemPerNode != tc.memory {
				t.Fatalf("memory reservation = %v, want %d MiB", ir.JobInfo.MemPerNode, tc.memory)
			}
		})
	}
}

func TestCPUDeviceAllocationStillUsesDeviceLimits(t *testing.T) {
	pod := podWithGPU(cpuDRADeviceClassExtendedName, "8")
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceName(cpuDRADeviceClassExtendedName): resource.MustParse("4"),
	}
	ir := &SlurmJobIR{Pods: corev1.PodList{Items: []corev1.Pod{pod}}}
	parsePodsCpuAndMemory(ir)
	if ir.JobInfo.CpuPerTask == nil || *ir.JobInfo.CpuPerTask != 8 {
		t.Fatalf("CPU DRA reservation = %v, want 8", ir.JobInfo.CpuPerTask)
	}
}
