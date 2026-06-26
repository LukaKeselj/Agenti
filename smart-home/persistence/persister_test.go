package persistence_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/LukaKeselj/Agenti/smart-home/persistence"
	"github.com/stretchr/testify/require"
)

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	require.NoError(t, err)

	type sensorState struct {
		Temp float64 `json:"temp"`
		Hum  float64 `json:"hum"`
	}

	original := sensorState{Temp: 23.5, Hum: 60.0}
	require.NoError(t, p.Save("sensor_kitchen", &original))

	var loaded sensorState
	require.NoError(t, p.Load("sensor_kitchen", &loaded))

	require.Equal(t, 23.5, loaded.Temp)
	require.Equal(t, 60.0, loaded.Hum)
}

func TestLoadNonExistent(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	require.NoError(t, err)

	var v any
	err = p.Load("ghost", &v)
	require.Error(t, err)
}

func TestExists(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	require.NoError(t, err)

	require.False(t, p.Exists("foo"))

	require.NoError(t, p.Save("foo", map[string]string{"x": "y"}))

	require.True(t, p.Exists("foo"))
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	require.NoError(t, err)

	p.Save("temp", "data")
	require.True(t, p.Exists("temp"))

	require.NoError(t, p.Delete("temp"))
	require.False(t, p.Exists("temp"))
}

func TestList(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	require.NoError(t, err)

	p.Save("a", 1)
	p.Save("b", 2)

	ids, err := p.List()
	require.NoError(t, err)

	require.Len(t, ids, 2)

	m := make(map[string]bool)
	for _, id := range ids {
		m[id] = true
	}
	require.True(t, m["a"])
	require.True(t, m["b"])
}

func TestAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	require.NoError(t, err)

	p.Save("atomic", "original")

	payload := ""
	for i := 0; i < 10000; i++ {
		payload += "The quick brown fox jumps over the lazy dog. "
	}
	require.NoError(t, p.Save("atomic", payload))

	var loaded string
	require.NoError(t, p.Load("atomic", &loaded))
	require.Equal(t, payload, loaded)
}

func TestCustomBasePath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "states")
	p, err := persistence.NewPersister(dir)
	require.NoError(t, err)

	p.Save("nested", "ok")

	_, err = os.Stat(filepath.Join(dir, "nested.json"))
	require.NoError(t, err)
}

func TestAppendJSON(t *testing.T) {
	dir := t.TempDir()
	p, err := persistence.NewPersister(dir)
	require.NoError(t, err)

	type entry struct {
		Round int     `json:"round"`
		MSE   float64 `json:"mse"`
	}

	require.NoError(t, p.AppendJSON("metrics", entry{Round: 1, MSE: 0.5}))
	require.NoError(t, p.AppendJSON("metrics", entry{Round: 2, MSE: 0.4}))
	require.NoError(t, p.AppendJSON("metrics", entry{Round: 3, MSE: 0.3}))

	// Verify the .jsonl file exists and has 3 lines.
	path := filepath.Join(dir, "metrics.jsonl")
	content, err := os.ReadFile(path)
	require.NoError(t, err)

	lines := 0
	for _, b := range content {
		if b == '\n' {
			lines++
		}
	}
	require.Equal(t, 3, lines, "expected 3 newline-delimited JSON entries")

	// Verify each line is valid JSON decodable back to entry.
	remaining := content
	for i := 1; i <= 3; i++ {
		newline := -1
		for j, b := range remaining {
			if b == '\n' {
				newline = j
				break
			}
		}
		require.True(t, newline >= 0)
		var e entry
		require.NoError(t, json.Unmarshal(remaining[:newline], &e))
		require.Equal(t, i, e.Round)
		remaining = remaining[newline+1:]
	}
}
