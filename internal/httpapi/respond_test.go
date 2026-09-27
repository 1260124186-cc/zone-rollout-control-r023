package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"solo-0014-zone-rollout-control/internal/allocation"
	"solo-0014-zone-rollout-control/internal/assessment"
	"solo-0014-zone-rollout-control/internal/fleet"
	"solo-0014-zone-rollout-control/internal/httpapi"
	"solo-0014-zone-rollout-control/internal/plan"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/probe"
	"solo-0014-zone-rollout-control/internal/store"
)

func newHandler(t *testing.T) http.Handler {
	t.Helper()
	stateStore, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	clock := platform.SystemClock{}
	ids := platform.CryptoIDGenerator{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	plans := plan.New(stateStore, clock, ids)
	fleetService := fleet.New(stateStore, clock, ids)
	allocations := allocation.New(stateStore, clock, ids)
	probes := probe.New(stateStore, clock, ids, allocations)
	assessments := assessment.New(stateStore, clock, ids)
	return httpapi.New(plans, fleetService, allocations, probes, assessments, ids, logger).Handler()
}

func validPlanBody() io.Reader {
	payload, _ := json.Marshal(map[string]any{
		"zone":                  "example.test",
		"minimum_success_ratio": 0.5,
		"maximum_latency_ms":    1000,
		"expected_nodes":        1,
	})
	return strings.NewReader(string(payload))
}

func TestCanceledRequestReturns499WithoutCommitting(t *testing.T) {
	handler := newHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/v1/plans", validPlanBody())
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != 499 {
		t.Fatalf("status: expected 499, got %d", recorder.Code)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Error.Code != "request_canceled" {
		t.Fatalf("error code: expected request_canceled, got %q", body.Error.Code)
	}
}

func TestDeadlineExceededRequestReturns504(t *testing.T) {
	handler := newHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/v1/plans", validPlanBody())
	ctx, cancel := context.WithDeadline(req.Context(), time.Now().Add(-time.Second))
	defer cancel()
	req = req.WithContext(ctx)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status: expected 504, got %d", recorder.Code)
	}
}
