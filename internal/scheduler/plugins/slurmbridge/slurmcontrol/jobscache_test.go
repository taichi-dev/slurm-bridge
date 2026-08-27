// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmcontrol

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/client/fake"
	"github.com/SlinkyProject/slurm-client/pkg/client/interceptor"
	"github.com/SlinkyProject/slurm-client/pkg/object"
)

type countingFetcher struct {
	mu      sync.Mutex
	calls   int
	result  map[string]ExternalJob
	err     error
	release chan struct{} // when non-nil, fetch blocks until closed
}

func (f *countingFetcher) fetch(ctx context.Context) (map[string]ExternalJob, error) {
	f.mu.Lock()
	f.calls++
	release := f.release
	f.mu.Unlock()
	if release != nil {
		<-release
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]ExternalJob, len(f.result))
	for k, v := range f.result {
		out[k] = v
	}
	return out, nil
}

func (f *countingFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newTestCache(f *countingFetcher) *jobsCache {
	c := newJobsCache(f.fetch)
	c.ttl = 50 * time.Millisecond
	return c
}

func TestJobsCache_ServesCachedWithinTTL(t *testing.T) {
	f := &countingFetcher{result: map[string]ExternalJob{"ns/pod1": {JobId: 1, Pending: true}}}
	c := newTestCache(f)

	for i := 0; i < 5; i++ {
		got, err := c.get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if (*got)["ns/pod1"].JobId != 1 {
			t.Fatalf("get() missing cached entry, got %v", *got)
		}
	}
	if f.callCount() != 1 {
		t.Fatalf("expected exactly 1 fetch within TTL, got %d", f.callCount())
	}
}

func TestJobsCache_ExpiredServesStaleAndRefreshesInBackground(t *testing.T) {
	f := &countingFetcher{result: map[string]ExternalJob{"ns/pod1": {JobId: 1, Pending: true}}}
	c := newTestCache(f)

	if _, err := c.get(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Age the snapshot past the TTL and change what Slurm would return.
	c.mu.Lock()
	c.fetchedAt = c.fetchedAt.Add(-time.Minute)
	c.mu.Unlock()
	f.mu.Lock()
	f.result = map[string]ExternalJob{"ns/pod2": {JobId: 2, Pending: true}}
	f.mu.Unlock()

	// The expired read must return immediately with the stale snapshot.
	got, err := c.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if (*got)["ns/pod1"].JobId != 1 {
		t.Fatalf("expired get() should serve stale data, got %v", *got)
	}

	// The background refresh should land shortly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := c.get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if (*got)["ns/pod2"].JobId == 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("background refresh never landed")
}

func TestJobsCache_UpsertVisibleImmediatelyAndSurvivesRacingRefresh(t *testing.T) {
	f := &countingFetcher{result: map[string]ExternalJob{}}
	c := newTestCache(f)

	if _, err := c.get(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Start a refresh that hangs mid-fetch (its List predates the upsert).
	release := make(chan struct{})
	f.mu.Lock()
	f.release = release
	f.mu.Unlock()
	c.mu.Lock()
	c.fetchedAt = c.fetchedAt.Add(-time.Minute)
	c.mu.Unlock()
	if _, err := c.get(context.Background()); err != nil { // kicks background refresh
		t.Fatal(err)
	}

	// Mutation lands while the refresh is in flight.
	time.Sleep(10 * time.Millisecond) // ensure upsert timestamp > refresh start
	c.upsert([]string{"ns/pod9"}, ExternalJob{JobId: 9, Pending: true})

	f.mu.Lock()
	f.release = nil
	f.mu.Unlock()
	close(release)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := c.get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if (*got)["ns/pod9"].JobId != 9 {
			t.Fatalf("upsert lost after racing refresh, got %v", *got)
		}
		c.mu.Lock()
		refreshing := c.refreshing
		c.mu.Unlock()
		if !refreshing {
			return // refresh completed and the upsert is still visible
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("refresh never completed")
}

func TestJobsCache_PurgeHidesDeletedJob(t *testing.T) {
	f := &countingFetcher{result: map[string]ExternalJob{
		"ns/pod1": {JobId: 1, Pending: true},
		"ns/pod2": {JobId: 2, Pending: true},
	}}
	c := newTestCache(f)

	if _, err := c.get(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.purge(1)
	got, err := c.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := (*got)["ns/pod1"]; ok {
		t.Fatalf("purged job still visible: %v", *got)
	}
	if (*got)["ns/pod2"].JobId != 2 {
		t.Fatalf("unrelated entry lost on purge: %v", *got)
	}
}

func TestJobsCache_NilSafe(t *testing.T) {
	var c *jobsCache
	c.upsert([]string{"ns/pod"}, ExternalJob{JobId: 1})
	c.purge(1)
}

func TestGetCachedJobsForPods_ServesFromCacheWithinTTL(t *testing.T) {
	lists := 0
	f := interceptor.Funcs{
		List: func(ctx context.Context, list object.ObjectList, opts ...client.ListOption) error {
			lists++
			return nil
		},
	}
	c := fake.NewClientBuilder().WithInterceptorFuncs(f).Build()
	r := NewControl(c, "kubernetes", "slurm-bridge")

	for i := 0; i < 5; i++ {
		if _, err := r.GetCachedJobsForPods(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if lists != 1 {
		t.Errorf("GetCachedJobsForPods() listed %d times within TTL, want 1", lists)
	}

	// The live variant must fetch every time.
	if _, err := r.GetJobsForPods(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lists != 2 {
		t.Errorf("GetJobsForPods() should always list; lists = %d, want 2", lists)
	}
}
