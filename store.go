package orchestrator

import "sync"

// RunRecord 一次修复运行的持久化记录。
type RunRecord struct {
	RunID       string
	EventID     string
	Source      string
	Kind        string
	EventStatus EventStatus
	RejectWhy   RejectReason
	Steps       []StepRecord
	Verdict     Verdict
	VerdictSet  bool // 冲突结论不得静默覆盖：只允许设置一次
}

// Terminal 判断运行是否已终结（不再推进）。
func (r RunRecord) Terminal() bool {
	switch r.EventStatus {
	case EventResolved, EventFailed, EventManual, EventRejected, EventSuppressed:
		return true
	}
	return false
}

// Store 本地状态存储接口。实现需保证并发安全。
type Store interface {
	// SaveRun 创建或整体替换一条运行记录。
	SaveRun(rec RunRecord) error
	// GetRun 按 RunID 查询。
	GetRun(runID string) (RunRecord, bool, error)
	// FindActiveRun 查找指定来源是否已有未终结的运行。
	FindActiveRun(source string) (RunRecord, bool, error)
	// AppendAudit 追加审计条目。
	AppendAudit(e AuditEntry) error
	// Audit 返回全部审计条目（按追加顺序）。
	Audit() ([]AuditEntry, error)
}

// MemoryStore 基于内存的 Store 实现（测试与默认使用）。
type MemoryStore struct {
	mu    sync.Mutex
	runs  map[string]RunRecord
	audit []AuditEntry
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{runs: make(map[string]RunRecord)}
}

func (s *MemoryStore) SaveRun(rec RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[rec.RunID] = rec
	return nil
}

func (s *MemoryStore) GetRun(runID string) (RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.runs[runID]
	return rec, ok, nil
}

func (s *MemoryStore) FindActiveRun(source string) (RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.runs {
		if rec.Source == source && !rec.Terminal() {
			return rec, true, nil
		}
	}
	return RunRecord{}, false, nil
}

func (s *MemoryStore) AppendAudit(e AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = append(s.audit, e)
	return nil
}

func (s *MemoryStore) Audit() ([]AuditEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AuditEntry, len(s.audit))
	copy(out, s.audit)
	return out, nil
}
