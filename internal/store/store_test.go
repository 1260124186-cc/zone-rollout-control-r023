package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	stateStore, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return stateStore
}

func TestWriteCanceledWhileWaitingDiscardsMutation(t *testing.T) {
	stateStore := openStore(t)

	// Hold the write lock the way an in-flight request would while another
	// request is waiting behind it.
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- stateStore.Write(context.Background(), func(state *domain.State) error {
			close(started)
			<-release
			state.Plans["pln-first"] = domain.RolloutPlan{ID: "pln-first"}
			return nil
		})
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- stateStore.Write(ctx, func(state *domain.State) error {
			state.Plans["pln-ghost"] = domain.RolloutPlan{ID: "pln-ghost"}
			return nil
		})
	}()

	// The client gives up while the server is still busy, then the first
	// request finishes.
	cancel()
	time.Sleep(20 * time.Millisecond)
	close(release)

	if err := <-firstDone; err != nil {
		t.Fatalf("first write: expected success, got %v", err)
	}
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write: expected context.Canceled, got %v", err)
	}

	if revision := stateStore.Revision(); revision != 1 {
		t.Fatalf("revision: expected only the committed write to count, got %d", revision)
	}
	err := stateStore.Read(context.Background(), func(state *domain.State) error {
		if _, ok := state.Plans["pln-first"]; !ok {
			t.Error("committed write must be visible")
		}
		if _, ok := state.Plans["pln-ghost"]; ok {
			t.Error("canceled write must not leave a business side effect")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
}

func TestWriteCanceledInsideCallbackDiscardsMutation(t *testing.T) {
	stateStore := openStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	err := stateStore.Write(ctx, func(state *domain.State) error {
		state.Plans["pln-ghost"] = domain.RolloutPlan{ID: "pln-ghost"}
		state.AuditEvents = append(state.AuditEvents, domain.AuditEvent{ID: "aud-ghost"})
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if revision := stateStore.Revision(); revision != 0 {
		t.Fatalf("revision: expected no commit, got %d", revision)
	}
	err = stateStore.Read(context.Background(), func(state *domain.State) error {
		if _, ok := state.Plans["pln-ghost"]; ok {
			t.Error("plan from canceled write must not be committed")
		}
		if len(state.AuditEvents) != 0 {
			t.Error("audit history must not record a canceled write")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
}

func TestWriteCommitPersistsAcrossReload(t *testing.T) {
	path := t.TempDir() + "/state.json"
	stateStore, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	err = stateStore.Write(context.Background(), func(state *domain.State) error {
		state.Plans["pln-done"] = domain.RolloutPlan{ID: "pln-done", State: domain.PlanActive}
		return nil
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	reopened, err := store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	err = reopened.Read(context.Background(), func(state *domain.State) error {
		plan, ok := state.Plans["pln-done"]
		if !ok {
			t.Fatal("committed plan must survive a reload")
		}
		if plan.State != domain.PlanActive {
			t.Fatalf("committed state mismatch: %q", plan.State)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read reopened state: %v", err)
	}
}

func TestWriteCanceledBeforeEntryDoesNotRun(t *testing.T) {
	stateStore := openStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := stateStore.Write(ctx, func(state *domain.State) error {
		t.Fatal("mutation callback must not run for an already canceled request")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if revision := stateStore.Revision(); revision != 0 {
		t.Fatalf("revision: expected no commit, got %d", revision)
	}
}

func TestReadCanceled(t *testing.T) {
	stateStore := openStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := stateStore.Read(ctx, func(state *domain.State) error {
		t.Fatal("read callback must not run for an already canceled request")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
