package criodiscovery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type dedupeStore interface {
	Seen(id string) bool
	Mark(id string) error
	Close() error
}

type noopDedupeStore struct{}

func newNoopDedupeStore() dedupeStore     { return noopDedupeStore{} }
func (noopDedupeStore) Seen(string) bool  { return false }
func (noopDedupeStore) Mark(string) error { return nil }
func (noopDedupeStore) Close() error      { return nil }

type fileCachePayload struct {
	Version int                       `json:"version"`
	Entries map[string]fileCacheEntry `json:"entries"`
}

type fileCacheEntry struct {
	LastNotifiedAt time.Time `json:"last_notified_at"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type fileDedupeStore struct {
	path    string
	ttl     time.Duration
	now     func() time.Time
	mu      sync.Mutex
	entries map[string]fileCacheEntry
}

func newDedupeStore(config Config) (dedupeStore, error) {
	if config.NotificationTTL <= 0 {
		return newNoopDedupeStore(), nil
	}
	return newFileDedupeStore(config.CachePath, config.NotificationTTL, time.Now)
}

func newFileDedupeStore(path string, ttl time.Duration, now func() time.Time) (*fileDedupeStore, error) {
	if now == nil {
		now = time.Now
	}
	store := &fileDedupeStore{
		path:    path,
		ttl:     ttl,
		now:     now,
		entries: map[string]fileCacheEntry{},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return nil, fmt.Errorf("read cache %s: %w", path, err)
	}
	var payload fileCachePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode cache %s: %w", path, err)
	}
	if payload.Version != 1 {
		return nil, fmt.Errorf("decode cache %s: unsupported version %d", path, payload.Version)
	}
	if payload.Entries != nil {
		store.entries = payload.Entries
	}
	store.pruneLocked(store.now())
	return store, nil
}

func (s *fileDedupeStore) Seen(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.pruneLocked(now)
	entry, ok := s.entries[id]
	return ok && entry.ExpiresAt.After(now)
}

func (s *fileDedupeStore) Mark(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.entries[id] = fileCacheEntry{LastNotifiedAt: now, ExpiresAt: now.Add(s.ttl)}
	s.pruneLocked(now)
	return s.saveLocked()
}

func (s *fileDedupeStore) Close() error { return nil }

func (s *fileDedupeStore) pruneLocked(now time.Time) {
	for id, entry := range s.entries {
		if !entry.ExpiresAt.After(now) {
			delete(s.entries, id)
		}
	}
}

func (s *fileDedupeStore) saveLocked() error {
	payload := fileCachePayload{Version: 1, Entries: s.entries}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("encode cache %s: %w", s.path, err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create cache directory %s: %w", filepath.Dir(s.path), err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write cache temp file %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace cache file %s: %w", s.path, err)
	}
	return nil
}
