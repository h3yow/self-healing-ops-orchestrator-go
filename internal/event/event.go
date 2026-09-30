package event

import (
	"time"
)

// Severity of a fault event.
type Severity string

const (
	SevLow      Severity = "low"
	SevHigh     Severity = "high"
	SevCritical Severity = "critical"
)

// Event is an incoming fault report.
type Event struct {
	ID         string            `json:"id"`
	Source     string            `json:"source"`
	FaultType  string            `json:"fault_type"`
	Target     string            `json:"target"`
	Severity   Severity          `json:"severity"`
	Message    string            `json:"message"`
	Labels     map[string]string `json:"labels,omitempty"`
	OccurredAt time.Time         `json:"occurred_at"`
	ReceivedAt time.Time         `json:"received_at"`
}

// RejectReason distinguishes rejection causes.
type RejectReason string

const (
	RejectSuppressed        RejectReason = "suppressed_duplicate"
	RejectMaintenanceWindow RejectReason = "maintenance_window"
	RejectPolicyNotMet      RejectReason = "policy_not_met"
)

// RejectedError carries an explicit, machine-readable reason.
type RejectedError struct {
	Reason  RejectReason
	Message string
}

func (e *RejectedError) Error() string { return string(e.Reason) + ": " + e.Message }

// AsRejected extracts a *RejectedError from err (nil otherwise).
func AsRejected(err error) *RejectedError {
	if err == nil {
		return nil
	}
	if r, ok := err.(*RejectedError); ok {
		return r
	}
	return nil
}

// IncidentStatus is the lifecycle state of a deduplicated fault.
type IncidentStatus string

const (
	IncidentActive      IncidentStatus = "active"
	IncidentSuppressed  IncidentStatus = "suppressed"
	IncidentRejected    IncidentStatus = "rejected"
	IncidentRemediating IncidentStatus = "remediating"
	IncidentResolved    IncidentStatus = "resolved"
	IncidentFailed      IncidentStatus = "failed"
)

// Incident is the merged, deduplicated fault record.
type Incident struct {
	ID            string         `json:"id"`
	SourceKey     string         `json:"source_key"`
	FaultType     string         `json:"fault_type"`
	Target        string         `json:"target"`
	Severity      Severity       `json:"severity"`
	Status        IncidentStatus `json:"status"`
	EventIDs      []string       `json:"event_ids"`
	FirstSeen     time.Time      `json:"first_seen"`
	LastSeen      time.Time      `json:"last_seen"`
	SuppressUntil time.Time      `json:"suppress_until"`
	RejectReason  RejectReason   `json:"reason,omitempty"`
	RejectMessage string         `json:"reject_message,omitempty"`
	LatestEvent   Event          `json:"latest_event"`
	ExecID        string         `json:"exec_id,omitempty"`
	Verdict       string         `json:"verdict,omitempty"`
	Version       int64          `json:"version"`
}

// SourceKey is the dedupe/merge identity of an event.
func SourceKey(e Event) string {
	return e.Source + "|" + e.FaultType + "|" + e.Target
}
