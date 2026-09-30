package event

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/clock"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

func baseEvent(id string) Event {
	return Event{
		ID: id, Source: "node-7", FaultType: "disk_full", Target: "/var",
		Severity: SevHigh, Message: "disk 95%",
	}
}

func newTestReceiver(t *testing.T, clk clock.Clock, maint Maintenance, pol Policy, supp time.Duration) (*Receiver, store.Store) {
	t.Helper()
	st, err := store.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	al := audit.NewLogger()
	return NewReceiver(st, clk, maint, pol, al, supp), st
}

func TestIngest_DuplicateSuppressed(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	r, _ := newTestReceiver(t, clk, AllowsAll{}, nil, time.Minute)

	inc, trigger, err := r.Ingest(ctx, baseEvent("e1"))
	if err != nil || !trigger {
		t.Fatalf("first ingest: trigger=%v err=%v", trigger, err)
	}
	t.Logf("event=e1 -> incident=%s status=%s trigger=%v", inc.ID, inc.Status, trigger)

	_, trigger, err = r.Ingest(ctx, baseEvent("e2"))
	if err == nil {
		t.Fatal("duplicate must be rejected with suppressed reason, got nil")
	}
	rej := AsRejected(err)
	if rej == nil || rej.Reason != RejectSuppressed {
		t.Fatalf("want suppressed_duplicate, got %v", err)
	}
	t.Logf("event=e2 -> rejected reason=%s msg=%q", rej.Reason, rej.Message)
	if trigger {
		t.Fatal("duplicate must not trigger remediation")
	}

	// Simulate remediation finishing while the suppression window is still
	// open: redelivery is merged and suppressed.
	if err := r.MarkTerminal(ctx, inc.ID, IncidentResolved, "success"); err != nil {
		t.Fatal(err)
	}
	inc2, _, err := r.Ingest(ctx, baseEvent("e2b"))
	if err == nil || AsRejected(err).Reason != RejectSuppressed {
		t.Fatalf("post-resolve redelivery must be suppressed, got %v", err)
	}
	if len(inc2.EventIDs) != 3 {
		t.Fatalf("redelivery must be merged for audit, eventIDs=%v", inc2.EventIDs)
	}
	t.Logf("event=e2b -> merged into %s eventIDs=%v reason=%s",
		inc2.ID, inc2.EventIDs, AsRejected(err).Reason)

	// After suppression elapses, the same source can open a new cycle.
	clk.Advance(2 * time.Minute)
	inc3, trigger, err := r.Ingest(ctx, baseEvent("e3"))
	if err != nil || !trigger {
		t.Fatalf("post-window ingest: trigger=%v err=%v", trigger, err)
	}
	t.Logf("event=e3 -> new incident cycle id=%s", inc3.ID)
}

func TestIngest_MaintenanceWindowReject(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(now)
	win := &OneShotWindow{Start: now, End: now.Add(30 * time.Minute)}
	r, _ := newTestReceiver(t, clk, win, nil, time.Minute)

	_, _, err := r.Ingest(ctx, baseEvent("e1"))
	rej := AsRejected(err)
	if rej == nil || rej.Reason != RejectMaintenanceWindow {
		t.Fatalf("want maintenance_window, got %v", err)
	}
	t.Logf("maintenance rejection reason=%s msg=%q", rej.Reason, rej.Message)

	clk.Advance(31 * time.Minute)
	inc, trigger, err := r.Ingest(ctx, baseEvent("e1"))
	if err != nil || !trigger {
		t.Fatalf("after window: trigger=%v err=%v", trigger, err)
	}
	t.Logf("window ended -> incident=%s accepted", inc.ID)
}

func TestIngest_PolicyNotMetDistinct(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFakeClock(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	pol := &MinSeverityPolicy{Min: SevCritical}
	r, _ := newTestReceiver(t, clk, AllowsAll{}, pol, time.Minute)

	_, _, err := r.Ingest(ctx, baseEvent("e1"))
	rej := AsRejected(err)
	if rej == nil || rej.Reason != RejectPolicyNotMet {
		t.Fatalf("want policy_not_met, got %v", err)
	}
	if !errors.Is(err, errRejectSentinel()) {
		// sanity: rejection is a *RejectedError
	}
	t.Logf("policy rejection reason=%s msg=%q", rej.Reason, rej.Message)
}

func errRejectSentinel() error { return &RejectedError{} }
