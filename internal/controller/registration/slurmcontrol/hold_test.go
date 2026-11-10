// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmcontrol

import (
	"context"
	"testing"

	"k8s.io/utils/ptr"

	api "github.com/SlinkyProject/slurm-client/api/v0044"
	"github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/client/fake"
	"github.com/SlinkyProject/slurm-client/pkg/client/interceptor"
	"github.com/SlinkyProject/slurm-client/pkg/object"

	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

func Test_realSlurmControl_HoldNode(t *testing.T) {
	drained := []api.V0044NodeState{api.V0044NodeStateDOWN, api.V0044NodeStateDRAIN}
	for _, tc := range []struct {
		name   string
		node   *api.V0044Node
		wantOn bool
	}{
		{name: "not drained", node: &api.V0044Node{Name: ptr.To("n1"), State: ptr.To([]api.V0044NodeState{api.V0044NodeStateDOWN})}, wantOn: true},
		// The bridge's own cordon drain is replaced, so lifting it on uncordon
		// cannot release the node while pods may still run.
		{name: "drained by slurm-bridge", node: &api.V0044Node{Name: ptr.To("n1"), State: ptr.To(drained), Reason: ptr.To("slurm-bridge: Corresponding Kubernetes node (n1) is unschedulable")}, wantOn: true},
		{name: "drained by an admin", node: &api.V0044Node{Name: ptr.To("n1"), State: ptr.To(drained), Reason: ptr.To("bad DIMM")}},
		{name: "already held", node: &api.V0044Node{Name: ptr.To("n1"), State: ptr.To(drained), Reason: ptr.To(wellknown.SlurmReasonLostHost)}},
		{name: "unknown to Slurm"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := fake.NewClientBuilder()
			if tc.node != nil {
				builder = builder.WithObjects(tc.node)
			}
			var updates []api.V0044UpdateNodeMsg
			c := builder.WithInterceptorFuncs(interceptor.Funcs{
				Update: func(_ context.Context, _ object.Object, req any, _ ...client.UpdateOption) error {
					updates = append(updates, req.(api.V0044UpdateNodeMsg))
					return nil
				},
			}).Build()

			if err := NewControl(c).HoldNode(t.Context(), "n1"); err != nil {
				t.Fatal(err)
			}
			if !tc.wantOn {
				if len(updates) != 0 {
					t.Fatalf("updates=%v, want none", updates)
				}
				return
			}
			if len(updates) != 1 || ptr.Deref(updates[0].Reason, "") != wellknown.SlurmReasonLostHost ||
				len(ptr.Deref(updates[0].State, nil)) != 1 || ptr.Deref(updates[0].State, nil)[0] != api.V0044UpdateNodeMsgStateDRAIN {
				t.Fatalf("updates=%v, want one drain with the lost-host reason", updates)
			}
		})
	}
}
