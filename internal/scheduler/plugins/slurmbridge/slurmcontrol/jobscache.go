// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmcontrol

import (
	"context"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

const (
	// jobsCacheTTL bounds how stale the pod->job map served to the scheduling
	// loop may be. The full /jobs listing is fetched and decoded at most once
	// per TTL, in the background (stale-while-revalidate), so the serial
	// scheduling loop never blocks on it. Consumers that mutate pod state
	// based on this map must confirm against a live single-job read first
	// (see SlurmBridge.validatePodToJob).
	jobsCacheTTL = 10 * time.Second
	// jobsCacheRefreshTimeout caps a single background refresh.
	jobsCacheRefreshTimeout = 60 * time.Second
)

type jobsFetchFunc func(ctx context.Context) (map[string]ExternalJob, error)

type jobsOverride struct {
	job ExternalJob
	at  time.Time
}

// jobsCache caches the pod->ExternalJob map built from the full Slurm /jobs
// listing. Mutations made through this scheduler (submit/update/delete) are
// applied to an overlay immediately, so the served map reflects them even
// when a background refresh raced with the mutation; overlay entries are
// dropped once a refresh that started after the mutation completes.
type jobsCache struct {
	mu    sync.Mutex
	fetch jobsFetchFunc
	ttl   time.Duration
	now   func() time.Time

	data       map[string]ExternalJob
	fetchedAt  time.Time
	refreshing bool

	overrides  map[string]jobsOverride
	tombstones map[int32]time.Time
}

func newJobsCache(fetch jobsFetchFunc) *jobsCache {
	return &jobsCache{
		fetch:      fetch,
		ttl:        jobsCacheTTL,
		now:        time.Now,
		overrides:  map[string]jobsOverride{},
		tombstones: map[int32]time.Time{},
	}
}

// get returns the pod->job map. A snapshot younger than the TTL is served
// as-is; an expired snapshot is still served, with a refresh kicked off in
// the background. Only the very first call fetches synchronously.
func (c *jobsCache) get(ctx context.Context) (*map[string]ExternalJob, error) {
	c.mu.Lock()
	if c.data == nil {
		c.mu.Unlock()
		fresh, err := c.fetch(ctx)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.data == nil {
			c.data = fresh
			c.fetchedAt = c.now()
		}
	} else if c.now().Sub(c.fetchedAt) > c.ttl && !c.refreshing {
		c.refreshing = true
		go c.refresh()
	}
	snapshot := c.mergedLocked()
	c.mu.Unlock()
	return &snapshot, nil
}

func (c *jobsCache) refresh() {
	started := c.now()
	ctx, cancel := context.WithTimeout(context.Background(), jobsCacheRefreshTimeout)
	defer cancel()
	fresh, err := c.fetch(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshing = false
	if err != nil {
		// Keep serving the stale snapshot; the next expired get retries.
		klog.Background().Error(err, "jobsCache: background refresh of the Slurm jobs listing failed; serving stale data",
			"age", c.now().Sub(c.fetchedAt))
		return
	}
	c.data = fresh
	c.fetchedAt = c.now()
	// A refresh that started after a mutation observed its effect in Slurm,
	// so overlay entries older than the refresh start are now redundant.
	for k, o := range c.overrides {
		if o.at.Before(started) {
			delete(c.overrides, k)
		}
	}
	for id, at := range c.tombstones {
		if at.Before(started) {
			delete(c.tombstones, id)
		}
	}
}

// mergedLocked builds the served snapshot: base data with overlay upserts
// applied and tombstoned jobs removed. Callers must hold c.mu.
func (c *jobsCache) mergedLocked() map[string]ExternalJob {
	out := make(map[string]ExternalJob, len(c.data)+len(c.overrides))
	for k, v := range c.data {
		if _, dead := c.tombstones[v.JobId]; dead {
			continue
		}
		out[k] = v
	}
	for k, o := range c.overrides {
		if _, dead := c.tombstones[o.job.JobId]; dead {
			continue
		}
		out[k] = o.job
	}
	return out
}

// upsert records a just-submitted or just-updated job under the given pod
// keys ("namespace/name") so the served map reflects it immediately.
func (c *jobsCache) upsert(podKeys []string, job ExternalJob) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.now()
	for _, k := range podKeys {
		c.overrides[k] = jobsOverride{job: job, at: at}
	}
	delete(c.tombstones, job.JobId)
}

// purge hides a just-deleted job from the served map until a refresh that
// postdates the deletion lands.
func (c *jobsCache) purge(jobId int32) {
	if c == nil || jobId == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tombstones[jobId] = c.now()
	for k, o := range c.overrides {
		if o.job.JobId == jobId {
			delete(c.overrides, k)
		}
	}
}
