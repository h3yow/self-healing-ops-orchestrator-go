package event

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/audit"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/clock"
	"github.com/h3yow/self-healing-ops-orchestrator-go/internal/store"
)

const incidentPrefix = "incidents/"

// Receiver accepts fault events, dedupes and merges them, and enforces
// suppression windows, maintenance windows and eligibility policy.
type Receiver struct {
	st          store.Store
	clk         clock.Clock
	maint       Maintenance
	policy      Policy
	audit       *audit.Logger
	suppression time.Duration

	mu sync.Mutex // serializes intake for deterministic dedupe under concurrency
}

func NewReceiver(st store.Store, clk clock.Clock, maint Maintenance,
	policy Policy, al *audit.Logger, suppression time.Duration) *Receiver {
	if maint == nil {
		maint = AllowsAll{}
	}
	return &Receiver{
		st:          st,
		clk:         clk,
		maint:       maint,
		policy:      policy,
		audit:       al,
		suppression: suppression,
	}
}

// Ingest dedupes/merges an event.
//
// Returns (incident, trigger, error):
//   - trigger=true  -> a brand-new accepted incident; caller should start remediation
//   - trigger=false, err=nil  -> merged into an existing open incident (suppressed)
//   - err=*RejectedError -> explicit rejection (maintenance window / policy / suppression)
func (r *Receiver) Ingest(ctx context.Context, e Event) (*Incident, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.clk.Now()
	if e.ReceivedAt.IsZero() {
		e.ReceivedAt = now
	}

	// 1. Maintenance window takes precedence: reject clearly, don't merge.
	if !r.maint.Allows(now) {
		win, _ := r.maint.ActiveWindow(now)
		return nil, false, r.reject(ctx, e, RejectMaintenanceWindow,
			fmt.Sprintf("maintenance window active until %s", win.End.Format(time.RFC3339)))
	}

	// 2. Eligibility policy.
	if r.policy != nil {
		if err := r.policy.Evaluate(e); err != nil {
			if rej := AsRejected(err); rej != nil {
				return nil, false, r.reject(ctx, e, rej.Reason, rej.Message)
			}
			return nil, false, r.reject(ctx, e, RejectPolicyNotMet, err.Error())
		}
	}

	// 3. Dedupe / merge against the stored incident for this source key.
	key := incidentPrefix + SourceKey(e)
	inc, version, err := store.LoadAs[Incident](ctx, r.st, key)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, false, err
	}
	exists := err == nil

	if exists {
		switch {
		case inc.Status == IncidentResolved && now.Before(inc.SuppressUntil):
			// Prior resolution is still in its suppression window: merge the
			// redelivery and reject with the distinct suppression reason.
			from := inc.Status
			inc.LastSeen = now
			inc.LatestEvent = e
			inc.EventIDs = appendUnique(inc.EventIDs, e.ID)
			inc.Status = IncidentSuppressed
			if _, err := store.SaveAs(ctx, r.st, key, version, inc); err != nil {
				return nil, false, err
			}
			r.logAudit(ctx, e, &inc, "incident", "event_merged_suppressed",
				string(from), string(inc.Status), "resolved incident still in suppression window")
			return &inc, false, r.reject(ctx, e, RejectSuppressed,
				fmt.Sprintf("resolved incident %s still suppressed until %s",
					inc.ID, inc.SuppressUntil.Format(time.RFC3339)))
		case inc.Status == IncidentResolved:
			// Prior fault fully closed and suppression elapsed: new cycle.
			inc = r.newIncident(e, now)
		case now.Before(inc.SuppressUntil) || inc.Status == IncidentActive ||
			inc.Status == IncidentRemediating:
			// Duplicate within suppression window, or while work is in flight.
			// Merge the redelivery for traceability, but reject it explicitly
			// with a distinct reason so remediation never retriggers.
			from := inc.Status
			inc.LastSeen = now
			inc.LatestEvent = e
			inc.EventIDs = appendUnique(inc.EventIDs, e.ID)
			if inc.Status != IncidentActive && inc.Status != IncidentRemediating {
				inc.Status = IncidentSuppressed
			}
			if _, err := store.SaveAs(ctx, r.st, key, version, inc); err != nil {
				return nil, false, err
			}
			r.logAudit(ctx, e, &inc, "incident", "event_merged_suppressed",
				string(from), string(inc.Status), "duplicate within suppression/open window")
			return &inc, false, r.reject(ctx, e, RejectSuppressed,
				fmt.Sprintf("duplicate of open incident %s; remediation already triggered/suppressed until %s",
					inc.ID, inc.SuppressUntil.Format(time.RFC3339)))
		default:
			// Terminal failed/rejected/suppressed cycle whose suppression
			// elapsed: overwrite with a new cycle at the same CAS version.
			inc = r.newIncident(e, now)
		}
	} else {
		inc = r.newIncident(e, now)
	}

	newVersion, err := store.SaveAs(ctx, r.st, key, version, inc)
	if err != nil {
		return nil, false, err
	}
	inc.Version = newVersion
	r.logAudit(ctx, e, &inc, "incident", "event_accepted", "", string(inc.Status),
		"new incident opened, remediation may start")
	return &inc, true, nil
}

func (r *Receiver) newIncident(e Event, now time.Time) Incident {
	suppressUntil := time.Time{}
	if r.suppression > 0 {
		suppressUntil = now.Add(r.suppression)
	}
	return Incident{
		ID:            "inc-" + e.ID,
		SourceKey:     SourceKey(e),
		FaultType:     e.FaultType,
		Target:        e.Target,
		Severity:      e.Severity,
		Status:        IncidentActive,
		EventIDs:      []string{e.ID},
		FirstSeen:     now,
		LastSeen:      now,
		SuppressUntil: suppressUntil,
		LatestEvent:   e,
	}
}

func (r *Receiver) reject(ctx context.Context, e Event, reason RejectReason, msg string) error {
	if r.audit != nil {
		_ = r.audit.Log(ctx, audit.Entry{
			EventID: e.ID,
			Entity:  "incident",
			Action:  "event_rejected",
			To:      string(reason),
			Reason:  msg,
			Detail:  map[string]string{"source": e.Source, "fault_type": e.FaultType, "target": e.Target},
		})
	}
	return &RejectedError{Reason: reason, Message: msg}
}

func (r *Receiver) logAudit(ctx context.Context, e Event, inc *Incident,
	entity, action, from, to, reason string) {
	if r.audit == nil {
		return
	}
	_ = r.audit.Log(ctx, audit.Entry{
		EventID:    e.ID,
		IncidentID: inc.ID,
		Entity:     entity,
		Action:     action,
		From:       from,
		To:         to,
		Reason:     reason,
	})
}

// keyForIncident maps an incident id back to its storage key via an index list.
// Incidents are stored by source key; id is deterministic ("inc-"+firstEventID).
// Callers that know the source key use keyBySource; Get scans when only id known.
func keyBySource(srcKey string) string { return incidentPrefix + srcKey }

// GetBySource loads an incident by source key.
func (r *Receiver) GetBySource(ctx context.Context, srcKey string) (*Incident, error) {
	inc, _, err := store.LoadAs[Incident](ctx, r.st, keyBySource(srcKey))
	if err != nil {
		return nil, err
	}
	return &inc, nil
}

// Get finds an incident by incident id.
func (r *Receiver) Get(ctx context.Context, id string) (*Incident, error) {
	all, err := r.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, inc := range all {
		if inc.ID == id {
			return inc, nil
		}
	}
	return nil, fmt.Errorf("%w: incident %s", store.ErrNotFound, id)
}

// List returns all incidents.
func (r *Receiver) List(ctx context.Context) ([]*Incident, error) {
	keys, err := r.st.List(ctx, incidentPrefix)
	if err != nil {
		return nil, err
	}
	out := make([]*Incident, 0, len(keys))
	for _, k := range keys {
		if strings.HasSuffix(k, ".tmp") {
			continue
		}
		inc, _, err := store.LoadAs[Incident](ctx, r.st, k)
		if err != nil {
			return nil, err
		}
		out = append(out, &inc)
	}
	return out, nil
}

// MarkRemediating moves an accepted incident into the remediating state.
func (r *Receiver) MarkRemediating(ctx context.Context, id, execID string) error {
	return r.update(ctx, id, func(inc *Incident) {
		inc.Status = IncidentRemediating
		inc.ExecID = execID
	}, "incident_mark_remediating")
}

// MarkTerminal closes an incident with a final status and verdict, extending
// suppression so redelivery cannot immediately retrigger.
func (r *Receiver) MarkTerminal(ctx context.Context, id string,
	status IncidentStatus, verdict string) error {
	return r.update(ctx, id, func(inc *Incident) {
		inc.Status = status
		inc.Verdict = verdict
		if r.suppression > 0 {
			inc.SuppressUntil = r.clk.Now().Add(r.suppression)
		}
	}, "incident_mark_terminal")
}

func (r *Receiver) update(ctx context.Context, id string,
	mutate func(*Incident), action string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	all, err := r.List(ctx)
	if err != nil {
		return err
	}
	for _, inc := range all {
		if inc.ID != id {
			continue
		}
		key := keyBySource(inc.SourceKey)
		_, version, err := store.LoadAs[Incident](ctx, r.st, key)
		if err != nil {
			return err
		}
		from := inc.Status
		mutate(inc)
		newVersion, err := store.SaveAs(ctx, r.st, key, version, *inc)
		if err != nil {
			return err
		}
		inc.Version = newVersion
		if r.audit != nil {
			_ = r.audit.Log(ctx, audit.Entry{
				EventID:    inc.LatestEvent.ID,
				IncidentID: inc.ID,
				ExecID:     inc.ExecID,
				Entity:     "incident",
				Action:     action,
				From:       string(from),
				To:         string(inc.Status),
				Reason:     inc.Verdict,
			})
		}
		return nil
	}
	return fmt.Errorf("%w: incident %s", store.ErrNotFound, id)
}

func appendUnique(xs []string, x string) []string {
	for _, v := range xs {
		if v == x {
			return xs
		}
	}
	return append(xs, x)
}
