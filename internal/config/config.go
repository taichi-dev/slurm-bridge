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
