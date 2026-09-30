package event

import "fmt"

// Policy decides whether an event is eligible for auto-remediation.
type Policy interface {
	Evaluate(e Event) error
}

// PolicyChain requires every policy to pass.
type PolicyChain struct {
	Rules []Policy
}

func (c PolicyChain) Evaluate(e Event) error {
	for _, p := range c.Rules {
		if err := p.Evaluate(e); err != nil {
			return err
		}
	}
	return nil
}

func sevRank(s Severity) int {
	switch s {
	case SevLow:
		return 1
	case SevHigh:
		return 2
	case SevCritical:
		return 3
	default:
		return 0
	}
}

// MinSeverityPolicy rejects events below a severity threshold.
type MinSeverityPolicy struct {
	Min Severity
}

func (p *MinSeverityPolicy) Evaluate(e Event) error {
	if sevRank(e.Severity) < sevRank(p.Min) {
		return &RejectedError{
			Reason:  RejectPolicyNotMet,
			Message: fmt.Sprintf("severity %q below required %q", e.Severity, p.Min),
		}
	}
	return nil
}

// AllowedFaultsPolicy only auto-remediates known fault types.
type AllowedFaultsPolicy struct {
	Allowed map[string]bool
}

func (p *AllowedFaultsPolicy) Evaluate(e Event) error {
	if !p.Allowed[e.FaultType] {
		return &RejectedError{
			Reason:  RejectPolicyNotMet,
			Message: fmt.Sprintf("fault type %q not allowed for auto remediation", e.FaultType),
		}
	}
	return nil
}
