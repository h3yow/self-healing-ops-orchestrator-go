package audit

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry is one immutable audit record. Every state transition carries the
// originating event ID and the execution ID so results stay traceable.
type Entry struct {
	Seq        int64       `json:"seq"`
	Time       time.Time   `json:"time"`
	EventID    string      `json:"event_id"`
	IncidentID string      `json:"incident_id,omitempty"`
	ExecID     string      `json:"exec_id,omitempty"`
	Entity     string      `json:"entity"`
	Action     string      `json:"action"`
	From       string      `json:"from,omitempty"`
	To         string      `json:"to,omitempty"`
	Reason     string      `json:"reason,omitempty"`
	Detail     interface{} `json:"detail,omitempty"`
}

// Sink receives audit entries.
type Sink interface {
	Write(ctx context.Context, e Entry) error
}

// Logger sequences and fans out audit entries.
type Logger struct {
	mu    sync.Mutex
	seq   int64
	sinks []Sink
}

func NewLogger(sinks ...Sink) *Logger { return &Logger{sinks: sinks} }

// Log assigns a monotonic sequence number, stamps the time if zero, and
// writes to every sink. It is safe for concurrent use.
func (l *Logger) Log(ctx context.Context, e Entry) error {
	l.mu.Lock()
	l.seq++
	if e.Seq == 0 {
		e.Seq = l.seq
	}
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	sinks := l.sinks
	l.mu.Unlock()
	for _, s := range sinks {
		if err := s.Write(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// Seq returns the latest sequence number.
func (l *Logger) Seq() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// FileSink appends one JSON object per line and fsyncs each write.
type FileSink struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func NewFileSink(path string) (*FileSink, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSink{f: f, w: bufio.NewWriter(f)}, nil
}

func (s *FileSink) Write(_ context.Context, e Entry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	if err := s.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Close()
}

// ReadEntries loads a JSONL audit file (helper for tests/inspection).
func ReadEntries(path string) ([]Entry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []Entry
	start := 0
	for i := 0; i <= len(b); i++ {
		if i == len(b) || b[i] == '\n' {
			line := b[start:i]
			start = i + 1
			if len(line) == 0 {
				continue
			}
			var e Entry
			if err := json.Unmarshal(line, &e); err != nil {
				return nil, err
			}
			out = append(out, e)
		}
	}
	return out, nil
}
