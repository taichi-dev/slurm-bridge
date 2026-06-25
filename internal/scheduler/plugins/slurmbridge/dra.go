// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-FileCopyrightText: Copyright 2024 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package slurmbridge

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/SlinkyProject/slurm-bridge/internal/nodeinfo"
	"github.com/SlinkyProject/slurm-bridge/internal/scheduler/plugins/slurmbridge/slurmcontrol"
)

// manageResourceClaim will create DRA ResourceClaims for each
// Slurm GRES type that matches a DRA DeviceClass name. Additionally,
// if the CPU DRA driver is installed, A ResourceClaim for CPUs will
// be generated. The ResourceClaim is reconstructed in a similar manner
// to the way the Kubernetes scheduler handles the DRA Extended Resource
// Claim capability.
func (sb *SlurmBridge) manageResourceClaim(ctx context.Context, pod *corev1.Pod, nodeName string, resources *slurmcontrol.NodeResources) error {
	claim, requestMappings, err := sb.createRequestsAndMappings(ctx, pod, nodeName, resources)
	if err != nil {
		return err
	}
	if claim == nil || requestMappings == nil {
		return nil
	}

	if err := sb.Create(ctx, claim); err != nil {
		var errs []error
		errs = append(errs, fmt.Errorf("create claim for extended resources %v: %w", klog.KObj(claim), err))

		if deleteErr := sb.Delete(ctx, claim); deleteErr != nil {
			errs = append(errs, fmt.Errorf("delete claim for extended resources %v: %w", klog.KObj(claim), deleteErr))
		}

		return utilerrors.NewAggregate(errs)
	}

	if err := sb.bindClaim(ctx, claim, pod, nodeName, resources); err != nil {
		var errs []error
		errs = append(errs, err)

		if deleteErr := sb.Delete(ctx, claim); deleteErr != nil {
			errs = append(errs, fmt.Errorf("delete claim for extended resources %v: %w", klog.KObj(claim), deleteErr))
		}

		return utilerrors.NewAggregate(errs)
	}

	if err := sb.patchPodExtendedResourceClaimStatus(ctx, pod, claim, requestMappings); err != nil {
		var errs []error
		errs = append(errs, err)

		if deleteErr := sb.Delete(ctx, claim); deleteErr != nil {
			errs = append(errs, fmt.Errorf("delete claim for extended resources %v: %w", klog.KObj(claim), deleteErr))
		}

		return utilerrors.NewAggregate(errs)
	}

	return nil
}

func (sb *SlurmBridge) createRequestsAndMappings(ctx context.Context, pod *corev1.Pod, nodeName string, resources *slurmcontrol.NodeResources) (*resourcev1.ResourceClaim, []corev1.ContainerExtendedResourceRequest, error) {
	if pod == nil {
		return nil, nil, errors.New("expected a pod to be given")
	}

	containers := slices.Clone(pod.Spec.InitContainers)
	containers = append(containers, pod.Spec.Containers...)

	// all mappings across all containers and resource types
	var mappings []corev1.ContainerExtendedResourceRequest

	nodeInfo, err := nodeinfo.NewNodeInfo(ctx, sb.Client, nodeName)
	if err != nil {
		return nil, nil, err
	}

	deviceRequests, err := nodeInfo.GetDeviceRequests(ctx, sb.Client, resources, sb.gpuTypeMap)
	if err != nil {
		return nil, nil, err
	}

	// The kubelet wires an allocated DRA device into a container by joining each
	// requestMapping to a device-request in the ResourceClaim *by name*
	// (mapping.RequestName must equal the claim request name, which is also the
	// allocation result's request). GetDeviceRequests names the requests "cpu"
	// (only when the CPU DRA driver is present) and gres.Name for each GRES, so
	// the mappings MUST reuse those exact names. A synthetic
	// "container-N-request-M" scheme never matches, so the device is allocated
	// but never CDI-injected (no /dev/nvidia*, "no NVIDIA driver" in-container).
	// Only emit a mapping when the claim actually carries a request of that name.
	claimRequestNames := make(map[string]bool, len(deviceRequests))
	for _, dr := range deviceRequests {
		claimRequestNames[dr.Name] = true
	}

	for _, container := range containers {
		for rName := range container.Resources.Requests {
			if rName.String() == corev1.ResourceCPU.String() {
				if claimRequestNames[corev1.ResourceCPU.String()] {
					mappings = append(mappings, corev1.ContainerExtendedResourceRequest{
						ContainerName: container.Name,
						RequestName:   corev1.ResourceCPU.String(),
						ResourceName:  corev1.ResourceCPU.String(),
					})
				}
				continue
			}
			for _, gres := range resources.Gres {
				deviceClass := nodeinfo.ResolveDeviceClass(sb.gpuTypeMap, gres.Type)
				if !strings.HasSuffix(rName.String(), deviceClass) {
					continue
				}
				if !claimRequestNames[gres.Name] {
					continue
				}
				mappings = append(mappings, corev1.ContainerExtendedResourceRequest{
					ContainerName: container.Name,
					RequestName:   gres.Name,
					ResourceName:  resourcev1.ResourceDeviceClassPrefix + deviceClass,
				})
			}
		}
	}

	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    pod.Namespace,
			GenerateName: pod.Name + "-extended-resources-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "v1",
					Kind:               "Pod",
					Name:               pod.Name,
					UID:                pod.UID,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				},
			},
			Annotations: map[string]string{
				resourcev1.ExtendedResourceClaimAnnotation: "true",
			},
		},
		Spec: resourcev1.ResourceClaimSpec{
			Devices: resourcev1.DeviceClaim{
				Requests: deviceRequests,
			},
		},
	}

	return claim, mappings, nil
}

// bindClaim gets called for claims which are not reserved for the pod yet.
// It might not even be allocated. bindClaim then ensures that the allocation
// and reservation are recorded.
func (sb *SlurmBridge) bindClaim(
	ctx context.Context,
	claim *resourcev1.ResourceClaim,
	pod *corev1.Pod,
	nodeName string,
	resources *slurmcontrol.NodeResources,
) error {
	nodeInfo, err := nodeinfo.NewNodeInfo(ctx, sb.Client, nodeName)
	if err != nil {
		return err
	}

	devices, err := nodeInfo.GetDeviceRequestAllocationResult(ctx, sb.Client, resources, sb.gpuTypeMap)
	if err != nil {
		return err
	}

	toUpdate := claim.DeepCopy()

	toUpdate.Status.Allocation = &resourcev1.AllocationResult{
		AllocationTimestamp: &metav1.Time{
			Time: time.Now(),
		},
		Devices: resourcev1.DeviceAllocationResult{
			Results: devices,
		},
		NodeSelector: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchFields: []corev1.NodeSelectorRequirement{
						{
							Key:      "metadata.name",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{nodeName},
						},
					},
				},
			},
		},
	}

	toUpdate.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{
		{Resource: "pods", Name: pod.Name, UID: pod.UID},
	}

	if err := sb.Status().Patch(ctx, toUpdate, client.StrategicMergeFrom(claim)); err != nil {
		return fmt.Errorf("failed to add reservation to claim %s status: %w", klog.KObj(claim), err)
	}

	if err := sb.Get(ctx, client.ObjectKeyFromObject(claim), claim); err != nil {
		return fmt.Errorf("failed to get claim %s: %w", klog.KObj(claim), err)
	}

	return nil
}

// patchPodExtendedResourceClaimStatus updates the pod's status with information about
// the extended resource claim.
func (sb *SlurmBridge) patchPodExtendedResourceClaimStatus(
	ctx context.Context,
	pod *corev1.Pod,
	claim *resourcev1.ResourceClaim,
	requestMappings []corev1.ContainerExtendedResourceRequest,
) error {
	if len(requestMappings) == 0 {
		return fmt.Errorf("nil or empty request mappings, no update of pod %s/%s ExtendedResourceClaimStatus", pod.Namespace, pod.Name)
	}

	toUpdate := pod.DeepCopy()
	toUpdate.Status.ExtendedResourceClaimStatus = &corev1.PodExtendedResourceClaimStatus{
		RequestMappings:   requestMappings,
		ResourceClaimName: claim.Name,
	}
	if err := sb.Status().Patch(ctx, toUpdate, client.StrategicMergeFrom(pod)); err != nil {
		return fmt.Errorf("failed to update pod %s ExtendedResourceClaimStatus: %w", klog.KObj(pod), err)
	}

	if err := sb.Get(ctx, client.ObjectKeyFromObject(toUpdate), toUpdate); err != nil {
		return fmt.Errorf("failed to get pod %s: %w", klog.KObj(pod), err)
	}

	return nil
}
