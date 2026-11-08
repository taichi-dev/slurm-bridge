// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	ConfigFile = "/etc/slurm-bridge/config.yaml"
)

type Config struct {
	SchedulerName            string                `yaml:"schedulerName"`
	SlurmRestApi             string                `yaml:"slurmRestApi"`
	ManagedNamespaces        []string              `yaml:"managedNamespaces"`
	ManagedNamespaceSelector *metav1.LabelSelector `yaml:"managedNamespaceSelector"`
	MCSLabel                 string                `yaml:"mcsLabel"`
	Partition                string                `yaml:"partition"`

	// KillOnNodeFail, when set, is sent as kill_on_node_fail on every external
	// job the scheduler submits. Unset leaves Slurm's default: the job is
	// killed when one of its nodes fails or goes DOWN. false keeps the job
	// running instead; with a slurmctld that retains external jobs submitted
	// with kill_on_node_fail=0 through a node DOWN (meshy-dev/slinky-containers
	// 25.11.8-1meshy2+), the job also keeps its whole allocation, so a slurmd
	// pod can be replaced without disrupting the bridged pods. Only set false
	// when slurmctld has that behaviour and every slurmd the bridge can place on
	// is host-bound (hostNetwork or DaemonSet NodeSets): otherwise a slurmd that
	// comes back on another host would carry the allocation with it.
	KillOnNodeFail *bool `yaml:"killOnNodeFail"`

	// GpuTypeMap maps a Slurm GPU GRES type name to a Kubernetes DRA
	// DeviceClass name. When slurmd runs with AutoDetect=nvidia, Slurm names
	// GPU GRES by model (e.g. "nvidia_b200", "h100"), which does not match the
	// DRA DeviceClass name ("gpu.nvidia.com"). This map lets operators declare
	// that a Slurm GRES type should be treated as a given DeviceClass, e.g.
	//
	//	gpuTypeMap:
	//	  nvidia_b200: gpu.nvidia.com
	//
	// When a Slurm GRES type has no entry, it is used as the DeviceClass name
	// directly, preserving the default behavior (Slurm type == DeviceClass).
	GpuTypeMap map[string]string `yaml:"gpuTypeMap"`
}

func Unmarshal(in []byte) (*Config, error) {
	out := &Config{}
	if err := yaml.Unmarshal(in, out); err != nil {
		return nil, err
	}
	return out, nil
}

func UnmarshalOrDie(in []byte) *Config {
	cfg, err := Unmarshal(in)
	if err != nil {
		panic(err)
	}
	return cfg
}
