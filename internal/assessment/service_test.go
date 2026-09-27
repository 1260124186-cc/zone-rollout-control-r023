package assessment_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"solo-0014-zone-rollout-control/internal/allocation"
	"solo-0014-zone-rollout-control/internal/assessment"
	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/fleet"
	"solo-0014-zone-rollout-control/internal/plan"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/probe"
	"solo-0014-zone-rollout-control/internal/store"
)

type fixture struct {
	store       *store.Store
	clock       *platform.ManualClock
	plans       *plan.Service
	fleet       *fleet.Service
	allocations *allocation.Service
	probes      *probe.Service
	assessments *assessment.Service
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
		store:       stateStore,
		clock:       clock,
		plans:       plan.New(stateStore, clock, ids),
		fleet:       fleet.New(stateStore, clock, ids),
		allocations: allocations,
		probes:      probe.New(stateStore, clock, ids, allocations),
		assessments: assessment.New(stateStore, clock, ids),
	}
}

// assessedPlan creates an active plan with one node that already reported a
// successful probe, so an assessment has input to aggregate.
func (f *fixture) assessedPlan(t *testing.T) string {
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
	group, err := f.allocations.Allocate(context.Background(), created.ID, domain.LeaseInput{
		RequestKey:   "lease-key-0001",
		Region:       "iad",
		LeaseSeconds: 600,
		MaxNodes:     1,
	})
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if _, err := f.probes.Submit(context.Background(), group.LeaseIDs[0], domain.ProbeInput{
		RequestKey:     "probe-key-0001",
		Success:        true,
		ObservedSerial: 2026092501,
		LatencyMS:      50,
		ObservedAt:     f.clock.Now(),
	}); err != nil {
		t.Fatalf("submit probe: %v", err)
	}
	return created.ID
}

func TestEvaluateCanceledLeavesNoSideEffects(t *testing.T) {
	f := newFixture(t)
	planID := f.assessedPlan(t)
	before, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.assessments.Evaluate(ctx, planID, domain.AssessmentInput{
		RequestKey: "assess-key-001",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	after, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(after.Assessments) != len(before.Assessments) {
		t.Error("canceled assessment must not be stored")
	}
	if len(after.AssessmentRequestKeys) != len(before.AssessmentRequestKeys) {
		t.Error("canceled assessment must not reserve the request key")
	}
	if len(after.AuditEvents) != len(before.AuditEvents) {
		t.Error("canceled assessment must not append audit history")
	}
	if after.Plans[planID].Version != before.Plans[planID].Version {
		t.Error("canceled assessment must not bump the plan version")
	}
	if after.Plans[planID].LatestAssessmentID != "" {
		t.Error("canceled assessment must not update the plan summary")
	}

	// The same key replays as a first-time request after cancellation.
	result, err := f.assessments.Evaluate(context.Background(), planID, domain.AssessmentInput{
		RequestKey: "assess-key-001",
	})
	if err != nil {
		t.Fatalf("retry after canceled assessment must succeed, got %v", err)
	}
	if result.PlanID != planID {
		t.Errorf("retry returned unexpected plan %q", result.PlanID)
	}
}

func TestEvaluateCommittedRequestReplaysOriginal(t *testing.T) {
	f := newFixture(t)
	planID := f.assessedPlan(t)

	input := domain.AssessmentInput{RequestKey: "assess-key-002"}
	first, err := f.assessments.Evaluate(context.Background(), planID, input)
	if err != nil {
		t.Fatalf("first evaluate: %v", err)
	}
	second, err := f.assessments.Evaluate(context.Background(), planID, input)
	if err != nil {
		t.Fatalf("replay evaluate: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay must return the original assessment: %q != %q", second.ID, first.ID)
	}
	state, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(state.Assessments) != 1 {
		t.Fatalf("replay must not store a second assessment, got %d", len(state.Assessments))
	}
	if state.Plans[planID].LatestAssessmentID != first.ID {
		t.Error("plan must keep pointing at the first assessment")
	}
}
