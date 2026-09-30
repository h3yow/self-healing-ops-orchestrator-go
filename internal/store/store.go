package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ErrNotFound is returned when a key is absent.
var ErrNotFound = errors.New("store: key not found")

// ErrConflict indicates an optimistic-concurrency version mismatch;
// callers use it to avoid silently overwriting concurrent conclusions.
var ErrConflict = errors.New("store: version conflict")

// Record wraps a value with an optimistic version.
type Record[T any] struct {
	Version int64 `json:"version"`
	Value   T     `json:"value"`
}

// Store is a small durable key/value store with versioned CAS.
type Store interface {
	// Save creates a record when rec.Version == 0 and the key is absent, or
	// overwrites when rec.Version matches the stored version. On success the
	// new version is returned.
	Save(ctx context.Context, key string, rec Record[[]byte]) (int64, error)
	Load(ctx context.Context, key string) (Record[[]byte], error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// FileStore persists one JSON file per key under a root directory.
type FileStore struct {
	root string
	mu   sync.Mutex
}

func NewFileStore(root string) (*FileStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{root: root}, nil
}

// keyPath prevents path traversal and maps "/" to directories.
func (s *FileStore) keyPath(key string) (string, error) {
	clean := filepath.Clean("/" + key)
	if strings.Contains(clean, "..") {
		return "", fmt.Errorf("store: invalid key %q", key)
	}
	return filepath.Join(s.root, clean), nil
}

type envelope struct {
	Version int64           `json:"version"`
	Value   json.RawMessage `json:"value"`
}

func (s *FileStore) Save(_ context.Context, key string, rec Record[[]byte]) (int64, error) {
	path, err := s.keyPath(key)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var current int64
	if b, err := os.ReadFile(path); err == nil {
		var env envelope
		if err := json.Unmarshal(b, &env); err != nil {
			return 0, fmt.Errorf("store: corrupt record %q: %w", key, err)
		}
		current = env.Version
	} else if !os.IsNotExist(err) {
		return 0, err
	} else {
		current = 0
	}

	if rec.Version != current {
		return 0, fmt.Errorf("%w: key=%s expected=%d actual=%d", ErrConflict, key, rec.Version, current)
	}
	newVersion := current + 1
	env := envelope{Version: newVersion, Value: json.RawMessage(rec.Value)}
	buf, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, err
	}
	if _, err := io.Copy(f, bytes.NewReader(buf)); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, err
	}
	// Best-effort directory fsync for durability of the rename.
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return newVersion, nil
}

func (s *FileStore) Load(_ context.Context, key string) (Record[[]byte], error) {
	path, err := s.keyPath(key)
	if err != nil {
		return Record[[]byte]{}, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Record[[]byte]{}, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return Record[[]byte]{}, err
	}
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return Record[[]byte]{}, fmt.Errorf("store: corrupt record %q: %w", key, err)
	}
	return Record[[]byte]{Version: env.Version, Value: []byte(env.Value)}, nil
}

func (s *FileStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	err := filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() || strings.HasSuffix(path, ".tmp") {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		keys = append(keys, key)
		return nil
	})
	if err != nil && os.IsNotExist(err) {
		return nil, nil
	}
	return keys, err
}

func (s *FileStore) Delete(_ context.Context, key string) error {
	path, err := s.keyPath(key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// SaveAs marshals v and performs a CAS save.
func SaveAs[T any](ctx context.Context, st Store, key string, version int64, v T) (int64, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return st.Save(ctx, key, Record[[]byte]{Version: version, Value: b})
}

// LoadAs reads and unmarshals a typed record.
func LoadAs[T any](ctx context.Context, st Store, key string) (T, int64, error) {
	var zero T
	rec, err := st.Load(ctx, key)
	if err != nil {
		return zero, 0, err
	}
	var v T
	if err := json.Unmarshal(rec.Value, &v); err != nil {
		return zero, 0, err
	}
	return v, rec.Version, nil
}
