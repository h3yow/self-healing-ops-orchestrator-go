package store

import (
	"context"
	"errors"
	"testing"
)

func TestSmoke(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	v, err := SaveAs(ctx, st, "incidents/x", 0, map[string]string{"a": "b"})
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("ver=%d", v)
	}
	if _, err := SaveAs(ctx, st, "incidents/x", 0, map[string]string{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	got, ver, err := LoadAs[map[string]string](ctx, st, "incidents/x")
	if err != nil {
		t.Fatal(err)
	}
	if ver != 1 || got["a"] != "b" {
		t.Fatalf("got %v ver %d", got, ver)
	}
	keys, err := st.List(ctx, "incidents/")
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	if _, err := st.Load(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want notfound, got %v", err)
	}
}
