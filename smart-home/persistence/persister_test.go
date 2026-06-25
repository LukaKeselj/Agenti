package persistence_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LukaKeselj/Agenti/smart-home/persistence"
)

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	type sensorState struct {
		Temp float64 `json:"temp"`
		Hum  float64 `json:"hum"`
	}

	original := sensorState{Temp: 23.5, Hum: 60.0}
	if err := p.Save("sensor_kitchen", &original); err != nil {
		t.Fatal(err)
	}

	var loaded sensorState
	if err := p.Load("sensor_kitchen", &loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Temp != 23.5 || loaded.Hum != 60.0 {
		t.Fatalf("expected (23.5, 60.0), got (%v, %v)", loaded.Temp, loaded.Hum)
	}
}

func TestLoadNonExistent(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	var v any
	err = p.Load("ghost", &v)
	if err == nil {
		t.Fatal("expected error for non-existent state, got nil")
	}
}

func TestExists(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	if p.Exists("foo") {
		t.Fatal("should not exist yet")
	}

	if err := p.Save("foo", map[string]string{"x": "y"}); err != nil {
		t.Fatal(err)
	}

	if !p.Exists("foo") {
		t.Fatal("should exist after save")
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	p.Save("temp", "data")
	if !p.Exists("temp") {
		t.Fatal("should exist after save")
	}

	if err := p.Delete("temp"); err != nil {
		t.Fatal(err)
	}
	if p.Exists("temp") {
		t.Fatal("should not exist after delete")
	}
}

func TestList(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	p.Save("a", 1)
	p.Save("b", 2)

	ids, err := p.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) != 2 {
		t.Fatalf("expected 2 IDs, got %d: %v", len(ids), ids)
	}

	m := make(map[string]bool)
	for _, id := range ids {
		m[id] = true
	}
	if !m["a"] || !m["b"] {
		t.Fatalf("expected [a b], got %v", ids)
	}
}

func TestAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	p.Save("atomic", "original")

	// Build a large valid UTF-8 string to test atomic overwrite.
	payload := ""
	for i := 0; i < 10000; i++ {
		payload += "The quick brown fox jumps over the lazy dog. "
	}
	if err := p.Save("atomic", payload); err != nil {
		t.Fatal(err)
	}

	var loaded string
	if err := p.Load("atomic", &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded != payload {
		t.Fatalf("corrupted data after overwrite: got %d bytes, expected %d",
			len(loaded), len(payload))
	}
}

func TestCustomBasePath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "states")
	p, err := persistence.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	p.Save("nested", "ok")

	if _, err := os.Stat(filepath.Join(dir, "nested.json")); err != nil {
		t.Fatalf("file not found at expected path: %v", err)
	}
}
