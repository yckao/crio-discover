package criodiscovery

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNoopCacheNeverSuppresses(t *testing.T) {
	cache := newNoopDedupeStore()
	if cache.Seen("container") {
		t.Fatal("noop cache should not suppress")
	}
	if err := cache.Mark("container"); err != nil {
		t.Fatalf("mark noop: %v", err)
	}
	if cache.Seen("container") {
		t.Fatal("noop cache should still not suppress")
	}
}

func TestFileCachePersistsAndExpires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	cache, err := newFileDedupeStore(path, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if cache.Seen("abc") {
		t.Fatal("new entry should not be seen")
	}
	if err := cache.Mark("abc"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if !cache.Seen("abc") {
		t.Fatal("entry should be seen after mark")
	}

	now = now.Add(30 * time.Minute)
	reloaded, err := newFileDedupeStore(path, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatalf("reload cache: %v", err)
	}
	if !reloaded.Seen("abc") {
		t.Fatal("entry should survive reload")
	}

	now = now.Add(31 * time.Minute)
	if reloaded.Seen("abc") {
		t.Fatal("entry should expire")
	}
}

func TestFileCacheRejectsCorruptJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatalf("write corrupt cache: %v", err)
	}
	_, err := newFileDedupeStore(path, time.Minute, time.Now)
	if err == nil || !strings.Contains(err.Error(), "decode cache") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestFileCacheWritesVersionedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	cache, err := newFileDedupeStore(path, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if err := cache.Mark("abc"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	var payload fileCachePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal cache: %v", err)
	}
	if payload.Version != 1 {
		t.Fatalf("version = %d", payload.Version)
	}
	entry := payload.Entries["abc"]
	if !entry.LastNotifiedAt.Equal(now) || !entry.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("entry = %#v", entry)
	}
}
