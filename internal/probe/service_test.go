package probe_test

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
	"solo-0014-zone-rollout-control/internal/probe"
	"solo-0014-zone-rollout-control/internal/store"
)

type fixture struct {
	store  *store.Store
	clock  *platform.ManualClock
	plans  *plan.Service
	fleet  *fleet.Service
	alloc  *allocation.Service
	probes *probe.Service
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	stateStore, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	clock := platform.NewManualClock(time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC))
	ids := platform.CryptoIDGenerator{}
	allocations := allocation.New(stateStore, clock, ids)
	return &fixture{
		store:  stateStore,
		clock:  clock,
		plans:  plan.New(stateStore, clock, ids),
		fleet:  fleet.New(stateStore, clock, ids),
		alloc:  allocations,
		probes: probe.New(stateStore, clock, ids, allocations),
	}
}

func (f *fixture) claimedLease(t *testing.T, leaseSeconds int) (string, string) {
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
	if _, err := f.fleet.Enroll(context.Background(), domain.NodeCreateInput{
		NodeName: "node-a",
		Region:   "iad",
	}); err != nil {
		t.Fatalf("enroll node: %v", err)
	}
	group, err := f.alloc.Allocate(context.Background(), created.ID, domain.LeaseInput{
		RequestKey:   "lease-key-0001",
		Region:       "iad",
		LeaseSeconds: leaseSeconds,
		MaxNodes:     1,
	})
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if len(group.LeaseIDs) != 1 {
		t.Fatalf("expected one lease, got %d", len(group.LeaseIDs))
	}
	return created.ID, group.LeaseIDs[0]
}

func (f *fixture) probeInput(requestKey string) domain.ProbeInput {
	return domain.ProbeInput{
		RequestKey:     requestKey,
		Success:        true,
		ObservedSerial: 2026092501,
		LatencyMS:      50,
		ObservedAt:     f.clock.Now(),
	}
}

func TestSubmitCanceledLeavesNoSideEffects(t *testing.T) {
	f := newFixture(t)
	_, leaseID := f.claimedLease(t, 600)
	before, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.probes.Submit(ctx, leaseID, f.probeInput("key-probe-001")); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	after, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(after.Probes) != len(before.Probes) {
		t.Error("canceled probe must not be stored")
	}
	if len(after.AuditEvents) != len(before.AuditEvents) {
		t.Error("canceled probe must not append audit history")
	}
	if got := after.Leases[leaseID].State; got != domain.LeaseClaimed {
		t.Errorf("lease state: expected claimed, got %q", got)
	}
	nodeID := after.Leases[leaseID].NodeID
	if got := after.Nodes[nodeID].State; got != domain.NodeLeased {
		t.Errorf("node state: expected leased, got %q", got)
	}

	// The same key replays as a first-time request after cancellation.
	result, err := f.probes.Submit(context.Background(), leaseID, f.probeInput("key-probe-001"))
	if err != nil {
		t.Fatalf("retry after canceled submit must succeed, got %v", err)
	}
	if result.LeaseID != leaseID {
		t.Errorf("retry returned unexpected lease %q", result.LeaseID)
	}
}

func TestSubmitCanceledDoesNotExpireLease(t *testing.T) {
	f := newFixture(t)
	_, leaseID := f.claimedLease(t, 60)
	f.clock.Advance(120 * time.Second)

	// The lease is now past its expiry time, but expiry housekeeping is part
	// of the probe transaction: a canceled request must not release the node.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.probes.Submit(ctx, leaseID, f.probeInput("key-probe-002")); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	state, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got := state.Leases[leaseID].State; got != domain.LeaseClaimed {
		t.Fatalf("lease state: expected claimed with no expiry side effect, got %q", got)
	}
	nodeID := state.Leases[leaseID].NodeID
	if got := state.Nodes[nodeID].State; got != domain.NodeLeased {
		t.Fatalf("node state: expected leased, got %q", got)
	}

	// A later request applies expiry inside its own transaction and rejects
	// the now-expired lease.
	_, err = f.probes.Submit(context.Background(), leaseID, f.probeInput("key-probe-003"))
	if domain.ErrorCodeOf(err) != domain.CodeLeaseNotClaimed {
		t.Fatalf("expected lease_not_claimed, got %v", err)
	}
	state, err = f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got := state.Leases[leaseID].State; got != domain.LeaseExpired {
		t.Fatalf("lease state: expected expired, got %q", got)
	}
	if got := state.Nodes[nodeID].State; got != domain.NodeReady {
		t.Fatalf("node state: expected ready, got %q", got)
	}
}

func TestSubmitCommittedRequestReplaysOriginal(t *testing.T) {
	f := newFixture(t)
	_, leaseID := f.claimedLease(t, 600)

	first, err := f.probes.Submit(context.Background(), leaseID, f.probeInput("key-probe-004"))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	second, err := f.probes.Submit(context.Background(), leaseID, f.probeInput("key-probe-004"))
	if err != nil {
		t.Fatalf("replay submit: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay must return the original probe: %q != %q", second.ID, first.ID)
	}
	state, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(state.Probes) != 1 {
		t.Fatalf("replay must not store a second probe, got %d", len(state.Probes))
	}
	if got := state.Leases[leaseID].State; got != domain.LeaseReported {
		t.Errorf("lease state: expected reported, got %q", got)
	}
}
