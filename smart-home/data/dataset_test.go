package data_test

import (
	"testing"

	"github.com/LukaKeselj/Agenti/smart-home/data"
)

func TestGenerateSamples_Count(t *testing.T) {
	samples := data.GenerateSamples("kitchen", 100, 0.5, false)
	if len(samples) != 100 {
		t.Fatalf("expected 100 samples, got %d", len(samples))
	}
}

func TestGenerateSamples_FeatureShape(t *testing.T) {
	samples := data.GenerateSamples("bedroom", 10, 0.0, false)
	for i, s := range samples {
		if len(s.Features) != 5 {
			t.Fatalf("sample %d: expected 5 features, got %d", i, len(s.Features))
		}
		if len(s.Target) != 3 {
			t.Fatalf("sample %d: expected 3 targets, got %d", i, len(s.Target))
		}
		if s.RoomID != "bedroom" {
			t.Fatalf("sample %d: expected room 'bedroom', got %q", i, s.RoomID)
		}
	}
}

func TestGenerateSamples_FeatureRanges(t *testing.T) {
	samples := data.GenerateSamples("test", 500, 0.5, false)
	for _, s := range samples {
		f := s.Features
		if f[0] < 15 || f[0] > 35 {
			t.Errorf("temp %.1f out of range [15, 35]", f[0])
		}
		if f[1] < 20 || f[1] > 80 {
			t.Errorf("hum %.1f out of range [20, 80]", f[1])
		}
		if f[2] < 50 || f[2] > 1000 {
			t.Errorf("light %.0f out of range [50, 1000]", f[2])
		}
		if f[3] != 0 && f[3] != 1 {
			t.Errorf("presence %.0f must be 0 or 1", f[3])
		}
		if f[4] < 0 || f[4] > 23 {
			t.Errorf("hour %.1f out of range [0, 23]", f[4])
		}
	}
}

func TestGenerateSensors_IID(t *testing.T) {
	rooms := []string{"kitchen", "bedroom", "livingroom"}
	datasets := data.GenerateSensors(rooms, 50, false)

	if len(datasets) != 3 {
		t.Fatalf("expected 3 rooms, got %d", len(datasets))
	}
	for _, room := range rooms {
		if len(datasets[room]) != 50 {
			t.Fatalf("room %s: expected 50 samples, got %d", room, len(datasets[room]))
		}
	}
}

func TestGenerateSensors_NonIID(t *testing.T) {
	rooms := []string{"cold-room", "hot-room"}
	datasets := data.GenerateSensors(rooms, 200, true)
	if len(datasets) != 2 {
		t.Fatalf("expected 2 rooms, got %d", len(datasets))
	}
	// non-IID: cold room should have lower avg temp than hot room.
	avgTemp := func(samples []data.Sample) float64 {
		var sum float64
		for _, s := range samples {
			sum += s.Features[0]
		}
		return sum / float64(len(samples))
	}
	coldAvg := avgTemp(datasets["cold-room"])
	hotAvg := avgTemp(datasets["hot-room"])
	if coldAvg >= hotAvg {
		t.Fatalf("expected cold-room (%.1f) < hot-room (%.1f)", coldAvg, hotAvg)
	}
}
