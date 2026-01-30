// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmjobir

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	// Ref: https://kubernetes.io/docs/concepts/workloads/pods/
	pod_v1 = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
)

func (t *translator) fromPod(pod *corev1.Pod) (*SlurmJobIR, error) {
	slurmJobIR := &SlurmJobIR{}
	slurmJobIR.Pods.Items = append(slurmJobIR.Pods.Items, *pod)
	tasks := int32(1)
	slurmJobIR.JobInfo.TasksPerNode = &tasks
	slurmJobIR.JobInfo.MaxNodes = &tasks
	slurmJobIR.JobInfo.MinNodes = &tasks
	return slurmJobIR, nil
}
