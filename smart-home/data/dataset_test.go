package data_test

import (
	"testing"

	"github.com/LukaKeselj/Agenti/smart-home/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateSamples_Count(t *testing.T) {
	samples := data.GenerateSamples("kitchen", 100, 0.5, false)
	require.Len(t, samples, 100)
}

func TestGenerateSamples_FeatureShape(t *testing.T) {
	samples := data.GenerateSamples("bedroom", 10, 0.0, false)
	for i, s := range samples {
		require.Lenf(t, s.Features, 5, "sample %d features", i)
		require.Lenf(t, s.Target, 3, "sample %d targets", i)
		require.Equal(t, "bedroom", s.RoomID)
	}
}

func TestGenerateSamples_FeatureRanges(t *testing.T) {
	samples := data.GenerateSamples("test", 500, 0.5, false)
	for _, s := range samples {
		f := s.Features
		assert.GreaterOrEqual(t, f[0], 0.0)
		assert.LessOrEqual(t, f[0], 1.0)
		assert.GreaterOrEqual(t, f[1], 0.0)
		assert.LessOrEqual(t, f[1], 1.0)
		assert.GreaterOrEqual(t, f[2], 0.0)
		assert.LessOrEqual(t, f[2], 1.0)
		assert.Contains(t, []float64{0, 1}, f[3])
		assert.GreaterOrEqual(t, f[4], 0.0)
		assert.LessOrEqual(t, f[4], 1.0)
	}
}

func TestGenerateSensors_IID(t *testing.T) {
	rooms := []string{"kitchen", "bedroom", "livingroom"}
	datasets := data.GenerateSensors(rooms, 50, false)

	require.Len(t, datasets, 3)
	for _, room := range rooms {
		require.Lenf(t, datasets[room], 50, "room %s", room)
	}
}

func TestGenerateSensors_NonIID(t *testing.T) {
	rooms := []string{"cold-room", "hot-room"}
	datasets := data.GenerateSensors(rooms, 200, true)
	require.Len(t, datasets, 2)

	avgTemp := func(samples []data.Sample) float64 {
		var sum float64
		for _, s := range samples {
			sum += s.Features[0]
		}
		return sum / float64(len(samples))
	}
	coldAvg := avgTemp(datasets["cold-room"])
	hotAvg := avgTemp(datasets["hot-room"])
	require.Less(t, coldAvg, hotAvg)
}
