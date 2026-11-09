// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package registration

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	kubefake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurmfake "github.com/SlinkyProject/slurm-client/pkg/client/fake"
	"github.com/SlinkyProject/slurm-client/pkg/client/interceptor"
	"github.com/SlinkyProject/slurm-client/pkg/object"

	"github.com/SlinkyProject/slurm-bridge/internal/dra"
	schedulercontrol "github.com/SlinkyProject/slurm-bridge/internal/scheduler/plugins/slurmbridge/slurmcontrol"
	"github.com/SlinkyProject/slurm-bridge/internal/utils/slurmjobir"
)

// TestNewReconcilerSubmissionOptions checks that the options given to
// NewReconciler reach the registrar that submits jobs, so a setting such as
// killOnNodeFail is sent on the submitted job.
func TestNewReconcilerSubmissionOptions(t *testing.T) {
	render := func(v *bool) string {
		if v == nil {
			return "nil"
		}
		return fmt.Sprint(*v)
	}
	for _, want := range []*bool{nil, ptr.To(false), ptr.To(true)} {
		t.Run("killOnNodeFail="+render(want), func(t *testing.T) {
			var got *bool
			submissions := 0
			slurm := interceptor.NewClient(slurmfake.NewClientBuilder().Build(), interceptor.Funcs{
				Create: func(_ context.Context, obj object.Object, request any, _ ...slurmclient.CreateOption) error {
					submissions++
					got = request.(api.V0044JobSubmitReq).Job.KillOnNodeFail
					obj.(*api.V0044JobInfo).JobId = ptr.To(int32(7))
					return nil
				},
			})
			var opts []schedulercontrol.Option
			if want != nil {
				opts = append(opts, schedulercontrol.WithKillOnNodeFail(want))
			}
			r := NewReconciler(kubefake.NewClientBuilder().Build(), slurm, "slurm-bridge", dra.DefaultRegistry(), nil, "", "", opts...)

			ir := &slurmjobir.SlurmJobIR{Components: []slurmjobir.SlurmJobComponent{{
				Pods: corev1.PodList{Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-0"}}}},
			}}}
			if _, err := r.submit.RegisterJob(t.Context(), ir); err != nil {
				t.Fatalf("RegisterJob() error = %v", err)
			}
			if submissions != 1 {
				t.Fatalf("submitted %d jobs, want 1", submissions)
			}
			if !ptr.Equal(got, want) {
				t.Errorf("kill_on_node_fail = %s, want %s", render(got), render(want))
			}
		})
	}
}
