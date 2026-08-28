// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmcontrol

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	api "github.com/SlinkyProject/slurm-client/api/v0044"
	"github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/client/fake"
	"github.com/SlinkyProject/slurm-client/pkg/client/interceptor"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	"github.com/SlinkyProject/slurm-client/pkg/types"

	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

func Test_realSlurmControl_GetJob(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		Client client.Client
	}
	type args struct {
		ctx context.Context
		pod *corev1.Pod
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "Job not found",
			fields: fields{
				Client: fake.NewFakeClient(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							wellknown.LabelExternalJobId: "1",
						},
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "Job found",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId:    ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{api.V0044JobInfoJobStateRUNNING},
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							wellknown.LabelExternalJobId: "1",
						},
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Job found but canceled",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId: ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{
								api.V0044JobInfoJobStateCANCELLED,
							},
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							wellknown.LabelExternalJobId: "1",
						},
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "Job found but completed",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId: ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{
								api.V0044JobInfoJobStateCOMPLETED,
							},
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							wellknown.LabelExternalJobId: "1",
						},
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "Stale cache shows completed, live read shows running",
			fields: fields{
				Client: fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, key object.ObjectKey, obj object.Object, opts ...client.GetOption) error {
						o := &client.GetOptions{}
						o.ApplyOptions(opts)
						state := api.V0044JobInfoJobStateCOMPLETED
						if o.RefreshCache {
							state = api.V0044JobInfoJobStateRUNNING
						}
						job := obj.(*types.V0044JobInfo)
						job.V0044JobInfo = api.V0044JobInfo{
							JobId:    ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{state},
						}
						return nil
					},
				}).Build(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							wellknown.LabelExternalJobId: "1",
						},
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Job missing from cache, live read shows running",
			fields: fields{
				Client: fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, key object.ObjectKey, obj object.Object, opts ...client.GetOption) error {
						o := &client.GetOptions{}
						o.ApplyOptions(opts)
						if !o.RefreshCache {
							return errors.New("not found")
						}
						job := obj.(*types.V0044JobInfo)
						job.V0044JobInfo = api.V0044JobInfo{
							JobId:    ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{api.V0044JobInfoJobStateRUNNING},
						}
						return nil
					},
				}).Build(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							wellknown.LabelExternalJobId: "1",
						},
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Cache shows running, no live read",
			fields: fields{
				Client: fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, key object.ObjectKey, obj object.Object, opts ...client.GetOption) error {
						o := &client.GetOptions{}
						o.ApplyOptions(opts)
						if o.RefreshCache {
							return errors.New("unexpected live read")
						}
						job := obj.(*types.V0044JobInfo)
						job.V0044JobInfo = api.V0044JobInfo{
							JobId:    ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{api.V0044JobInfoJobStateRUNNING},
						}
						return nil
					},
				}).Build(),
			},
			args: args{
				ctx: ctx,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							wellknown.LabelExternalJobId: "1",
						},
					},
				},
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &realSlurmControl{
				Client: tt.fields.Client,
			}
			got, err := r.IsJobRunning(tt.args.ctx, tt.args.pod)
			if (err != nil) != tt.wantErr {
				t.Errorf("realSlurmControl.IsJobRunning() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("realSlurmControl.IsJobRunning() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_realSlurmControl_IsJobPendingOrRunning(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		Client client.Client
	}
	type args struct {
		ctx   context.Context
		jobId int32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "job not found",
			fields: fields{
				Client: fake.NewFakeClient(),
			},
			args: args{
				ctx:   ctx,
				jobId: 1,
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "job pending",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId:    ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{api.V0044JobInfoJobStatePENDING},
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{ctx: ctx, jobId: 1},
			want: true,
		},
		{
			name: "job running",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId:    ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{api.V0044JobInfoJobStateRUNNING},
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{ctx: ctx, jobId: 1},
			want: true,
		},
		{
			name: "job pending and running (multiple states)",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId: ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{
								api.V0044JobInfoJobStatePENDING,
								api.V0044JobInfoJobStateRUNNING,
							},
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{ctx: ctx, jobId: 1},
			want: true,
		},
		{
			name: "job completed",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId:    ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{api.V0044JobInfoJobStateCOMPLETED},
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{ctx: ctx, jobId: 1},
			want: false,
		},
		{
			name: "job canceled",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId:    ptr.To[int32](1),
							JobState: &[]api.V0044JobInfoJobState{api.V0044JobInfoJobStateCANCELLED},
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{ctx: ctx, jobId: 1},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &realSlurmControl{
				Client: tt.fields.Client,
			}
			got, err := r.IsJobPendingOrRunning(tt.args.ctx, tt.args.jobId)
			if (err != nil) != tt.wantErr {
				t.Errorf("realSlurmControl.IsJobPendingOrRunning() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("realSlurmControl.IsJobPendingOrRunning() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_realSlurmControl_TerminateJob(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		Client client.Client
	}
	type args struct {
		ctx   context.Context
		jobId int32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Job not found",
			fields: fields{
				Client: fake.NewFakeClient(),
			},
			args: args{
				ctx:   ctx,
				jobId: 0,
			},
			wantErr: false,
		},
		{
			name: "Job deleted",
			fields: fields{
				Client: func() client.Client {
					obj := &types.V0044JobInfo{
						V0044JobInfo: api.V0044JobInfo{
							JobId: ptr.To[int32](1),
						},
					}
					return fake.NewClientBuilder().WithObjects(obj).Build()
				}(),
			},
			args: args{
				ctx:   ctx,
				jobId: 1,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &realSlurmControl{
				Client: tt.fields.Client,
			}
			if err := r.TerminateJob(tt.args.ctx, tt.args.jobId); (err != nil) != tt.wantErr {
				t.Errorf("realSlurmControl.TerminateJob() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
