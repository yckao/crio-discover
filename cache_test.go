package discover

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
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

func TestFileCacheFailedMarkDoesNotSuppress(t *testing.T) {
	dir := t.TempDir()
	notDir := filepath.Join(dir, "not-dir")
	if err := os.WriteFile(notDir, []byte("file"), 0o600); err != nil {
		t.Fatalf("write non-directory: %v", err)
	}
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	cache := &fileDedupeStore{
		path:    filepath.Join(notDir, "cache.json"),
		ttl:     time.Hour,
		now:     func() time.Time { return now },
		entries: map[string]fileCacheEntry{},
	}
	if err := cache.Mark("abc"); err == nil {
		t.Fatal("expected mark to fail")
	}
	if cache.Seen("abc") {
		t.Fatal("failed mark should not suppress entry")
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

func TestFileCacheWritesOwnerOnlyPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("windows does not report POSIX mode bits")
	}
	path := filepath.Join(t.TempDir(), "cache.json")
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	cache, err := newFileDedupeStore(path, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if err := cache.Mark("abc"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat cache: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("cache permissions = %o, want 600", got)
	}
}

func TestFileCacheDoesNotReusePredictableTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache.json")
	tmpPath := path + ".tmp"
	sentinel := []byte("sentinel")
	if err := os.WriteFile(tmpPath, sentinel, 0o600); err != nil {
		t.Fatalf("write predictable temp file: %v", err)
	}
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	cache, err := newFileDedupeStore(path, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if err := cache.Mark("abc"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	got, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatalf("read predictable temp file: %v", err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Fatalf("predictable temp file was modified: got %q, want %q", got, sentinel)
	}
}
