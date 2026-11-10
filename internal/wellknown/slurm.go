// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package wellknown

const (
	// SlurmFeatureGRESCompatible identifies nodes which the bridge has verified
	// can represent their Kubernetes DRA inventory in Slurm.
	SlurmFeatureGRESCompatible = "slurm_bridge_gres_compatible"

	// SlurmReasonLostHost is the drain reason with which the bridge keeps a
	// Slurm node out of service after releasing an allocation whose pod on the
	// node's lost host may still run. The node controller lifts it once no
	// deleted bridged pod remains bound to the node.
	SlurmReasonLostHost = "slurm-bridge: waiting for pods on a lost host to stop"
)
