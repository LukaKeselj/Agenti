package persistence

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Persister provides JSON-based save/load for actor state.
// Each actor's state is stored in a separate file named {ActorID}.json
// under a configurable base path.
type Persister struct {
	basePath string
	mu       sync.RWMutex
}

// NewPersister creates a new Persister rooted at basePath.
// The directory is created automatically if it does not exist.
func NewPersister(basePath string) (*Persister, error) {
	abs, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("persister: invalid path %q: %w", basePath, err)
	}
	if err := os.MkdirAll(abs, 0755); err != nil {
		return nil, fmt.Errorf("persister: cannot create directory %q: %w", abs, err)
	}
	return &Persister{basePath: abs}, nil
}

// Save serialises state to {basePath}/{id}.json atomically
// (writes to a temp file, then renames).
func (p *Persister) Save(id string, state any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("persister: marshal failed for %q: %w", id, err)
	}

	path := p.filePath(id)
	tmp := path + ".tmp"

	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("persister: write failed for %q: %w", id, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("persister: rename failed for %q: %w", id, err)
	}
	return nil
}

// Load deserialises state from {basePath}/{id}.json into dest.
// dest must be a pointer to the target type.
func (p *Persister) Load(id string, dest any) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	path := p.filePath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("persister: no saved state for %q", id)
		}
		return fmt.Errorf("persister: read failed for %q: %w", id, err)
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("persister: unmarshal failed for %q: %w", id, err)
	}
	return nil
}

// Exists returns true if a saved state file exists for the given id.
func (p *Persister) Exists(id string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, err := os.Stat(p.filePath(id))
	return err == nil
}

// Delete removes the saved state file for the given id.
func (p *Persister) Delete(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return os.Remove(p.filePath(id))
}

// List returns all actor IDs that have saved state files.
func (p *Persister) List() ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	entries, err := os.ReadDir(p.basePath)
	if err != nil {
		return nil, fmt.Errorf("persister: read dir failed: %w", err)
	}
	var ids []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			ids = append(ids, trimExt(e.Name()))
		}
	}
	return ids, nil
}

// ── internal ───────────────────────────────────────────────────

func (p *Persister) filePath(id string) string {
	return filepath.Join(p.basePath, id+".json")
}

func trimExt(name string) string {
	return name[:len(name)-len(filepath.Ext(name))]
}
