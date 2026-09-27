package allocation_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"solo-0014-zone-rollout-control/internal/allocation"
	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/fleet"
	"solo-0014-zone-rollout-control/internal/plan"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type fixture struct {
	store *store.Store
	clock *platform.ManualClock
	plans *plan.Service
	fleet *fleet.Service
	alloc *allocation.Service
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	stateStore, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	clock := platform.NewManualClock(time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC))
	ids := platform.CryptoIDGenerator{}
	return &fixture{
		store: stateStore,
		clock: clock,
		plans: plan.New(stateStore, clock, ids),
		fleet: fleet.New(stateStore, clock, ids),
		alloc: allocation.New(stateStore, clock, ids),
	}
}

func (f *fixture) activePlan(t *testing.T) string {
	t.Helper()
	created, err := f.plans.Create(context.Background(), domain.PlanCreateInput{
		Zone:                "example.test",
		MinimumSuccessRatio: 0.5,
		MaximumLatencyMS:    1000,
		ExpectedNodes:       1,
	})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if _, err := f.plans.AddRevision(context.Background(), created.ID, domain.RevisionCreateInput{
		Serial:    2026092501,
		Authority: "ns1.example.test",
		Entries: []domain.ZoneEntry{
			{Owner: "example.test", Type: domain.EntryA, Value: "192.0.2.10"},
		},
	}); err != nil {
		t.Fatalf("add revision: %v", err)
	}
	if _, err := f.plans.Activate(context.Background(), created.ID, 2); err != nil {
		t.Fatalf("activate plan: %v", err)
	}
	return created.ID
}

func (f *fixture) enrollNode(t *testing.T, name string) domain.ResolverNode {
	t.Helper()
	node, err := f.fleet.Enroll(context.Background(), domain.NodeCreateInput{
		NodeName: name,
		Region:   "iad",
	})
	if err != nil {
		t.Fatalf("enroll node: %v", err)
	}
	return node
}

func leaseInput(requestKey string) domain.LeaseInput {
	return domain.LeaseInput{
		RequestKey:   requestKey,
		Region:       "iad",
		LeaseSeconds: 600,
		MaxNodes:     1,
	}
}

func (f *fixture) snapshot(t *testing.T) domain.State {
	t.Helper()
	state, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot state: %v", err)
	}
	return state
}

func TestAllocateCanceledLeavesNoSideEffects(t *testing.T) {
	f := newFixture(t)
	planID := f.activePlan(t)
	node := f.enrollNode(t, "node-a")
	before := f.snapshot(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.alloc.Allocate(ctx, planID, leaseInput("key-cancel-01")); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	after := f.snapshot(t)
	if len(after.Leases) != len(before.Leases) {
		t.Error("canceled allocation must not create leases")
	}
	if len(after.LeaseGroups) != len(before.LeaseGroups) {
		t.Error("canceled allocation must not create a lease group")
	}
	if len(after.LeaseRequestKeys) != len(before.LeaseRequestKeys) {
		t.Error("canceled allocation must not reserve the request key")
	}
	if len(after.AuditEvents) != len(before.AuditEvents) {
		t.Error("canceled allocation must not append audit history")
	}
	if got := after.Nodes[node.ID].State; got != domain.NodeReady {
		t.Errorf("node state: expected ready, got %q", got)
	}

	// A fresh replay with the same key must still work and allocate the
	// node: cancellation must not poison idempotency state.
	group, err := f.alloc.Allocate(context.Background(), planID, leaseInput("key-cancel-01"))
	if err != nil {
		t.Fatalf("retry after canceled attempt must succeed, got %v", err)
	}
	if len(group.LeaseIDs) != 1 {
		t.Fatalf("expected one lease, got %d", len(group.LeaseIDs))
	}
}

func TestAllocateCanceledWhileStoreBusyLeavesNoSideEffects(t *testing.T) {
	f := newFixture(t)
	planID := f.activePlan(t)
	node := f.enrollNode(t, "node-b")

	// Another request holds the store lock, reproducing a busy server.
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = f.store.Write(context.Background(), func(state *domain.State) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := f.alloc.Allocate(ctx, planID, leaseInput("key-busy-0002"))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	close(release)

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	after := f.snapshot(t)
	if len(after.Leases) != 0 || len(after.LeaseGroups) != 0 || len(after.LeaseRequestKeys) != 0 {
		t.Error("canceled request that waited on the lock must not commit anything")
	}
	if got := after.Nodes[node.ID].State; got != domain.NodeReady {
		t.Errorf("node state: expected ready, got %q", got)
	}

	// Retry after cancellation behaves as a first-time request.
	group, err := f.alloc.Allocate(context.Background(), planID, leaseInput("key-busy-0002"))
	if err != nil {
		t.Fatalf("retry after canceled attempt must succeed, got %v", err)
	}
	if len(group.LeaseIDs) != 1 {
		t.Fatalf("expected one lease, got %d", len(group.LeaseIDs))
	}
}

func TestAllocateCommittedRequestReplaysOriginal(t *testing.T) {
	f := newFixture(t)
	planID := f.activePlan(t)
	f.enrollNode(t, "node-c")

	first, err := f.alloc.Allocate(context.Background(), planID, leaseInput("key-replay-001"))
	if err != nil {
		t.Fatalf("first allocate: %v", err)
	}
	second, err := f.alloc.Allocate(context.Background(), planID, leaseInput("key-replay-001"))
	if err != nil {
		t.Fatalf("replay allocate: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay must return the original group: %q != %q", second.ID, first.ID)
	}
	state := f.snapshot(t)
	if len(state.Leases) != 1 {
		t.Fatalf("replay must not create additional leases, got %d", len(state.Leases))
	}
}
