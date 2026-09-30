package event

import "time"

// Window is a maintenance interval during which remediation is forbidden.
type Window struct {
	Start time.Time
	End   time.Time
}

// Maintenance decides whether remediation is currently allowed.
type Maintenance interface {
	Allows(t time.Time) bool
	ActiveWindow(t time.Time) (Window, bool)
}

// AllowsAll is the default "no maintenance" policy.
type AllowsAll struct{}

func (AllowsAll) Allows(time.Time) bool                 { return true }
func (AllowsAll) ActiveWindow(time.Time) (Window, bool) { return Window{}, false }

// OneShotWindow forbids remediation during one absolute interval.
type OneShotWindow struct {
	Start, End time.Time
}

func (w *OneShotWindow) Allows(t time.Time) bool {
	return t.Before(w.Start) || !t.Before(w.End)
}

func (w *OneShotWindow) ActiveWindow(t time.Time) (Window, bool) {
	if !w.Allows(t) {
		return Window{Start: w.Start, End: w.End}, true
	}
	return Window{}, false
}

// DailyWindow recurs every day between two local clock times.
// If End <= Start the window wraps past midnight.
type DailyWindow struct {
	HourStart, MinStart int
	HourEnd, MinEnd     int
	Location            *time.Location
}

func (w *DailyWindow) startEnd(day time.Time) (time.Time, time.Time) {
	loc := w.Location
	if loc == nil {
		loc = day.Location()
	}
	d := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, loc)
	start := d.Add(time.Duration(w.HourStart)*time.Hour + time.Duration(w.MinStart)*time.Minute)
	end := d.Add(time.Duration(w.HourEnd)*time.Hour + time.Duration(w.MinEnd)*time.Minute)
	if !end.After(start) {
		end = end.Add(24 * time.Hour)
	}
	return start, end
}

func (w *DailyWindow) ActiveWindow(t time.Time) (Window, bool) {
	// Check today's interval and yesterday's (wrapped) interval.
	for _, day := range []time.Time{t, t.Add(-24 * time.Hour)} {
		start, end := w.startEnd(day)
		if !t.Before(start) && t.Before(end) {
			return Window{Start: start, End: end}, true
		}
	}
	return Window{}, false
}

func (w *DailyWindow) Allows(t time.Time) bool {
	_, active := w.ActiveWindow(t)
	return !active
}

// MaintenanceChain denies if any member window is active.
type MaintenanceChain struct {
	Windows []Maintenance
}

func (c MaintenanceChain) Allows(t time.Time) bool {
	for _, w := range c.Windows {
		if !w.Allows(t) {
			return false
		}
	}
	return true
}

func (c MaintenanceChain) ActiveWindow(t time.Time) (Window, bool) {
	for _, w := range c.Windows {
		if win, ok := w.ActiveWindow(t); ok {
			return win, true
		}
	}
	return Window{}, false
}
