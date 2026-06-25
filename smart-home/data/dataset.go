package data

import (
	"math/rand"
)

// Sample is a single training example for the smart-home model.
type Sample struct {
	Features []float64 // [temp, humidity, light, presence, hour]
	Target   []float64 // [target_temp, target_hum, target_light]
	RoomID   string
}

// GenerateSensors returns a map of roomID → []Sample.
// Each sensor gets roughly equalSamples examples.
// When nonIID is true, each room's data distribution is deliberately skewed
// (e.g. kitchen runs hotter, bedroom is cooler).
func GenerateSensors(roomIDs []string, samplesPerSensor int, nonIID bool) map[string][]Sample {
	result := make(map[string][]Sample, len(roomIDs))
	for i, room := range roomIDs {
		skew := float64(i) / float64(len(roomIDs)-1) // 0.0 .. 1.0
		result[room] = GenerateSamples(room, samplesPerSensor, skew, nonIID)
	}
	return result
}

// GenerateSamples creates numSamples synthetic IoT readings for a room.
// skew controls the room's baseline (0.0 = cold/dark, 1.0 = hot/bright).
func GenerateSamples(roomID string, numSamples int, skew float64, nonIID bool) []Sample {
	samples := make([]Sample, numSamples)
	for i := 0; i < numSamples; i++ {
		hour := clamp(rand.Float64()*24.0, 0, 23)
		presence := 0.0
		if hour > 7 && hour < 22 && rand.Float64() < 0.7 {
			presence = 1.0
		}
		if hour > 22 || hour < 6 {
			presence = 0.0
		}

		baseTemp := 20.0 + skew*10.0 // 20°C (cold room) … 30°C (hot room)
		if !nonIID {
			baseTemp = 25.0 // all rooms have same baseline in IID mode
		}
		temp := clamp(baseTemp+rand.NormFloat64()*2.0, 15, 35)

		baseHum := 50.0
		hum := clamp(baseHum+rand.NormFloat64()*10.0, 20, 80)

		baseLight := 200.0 + skew*600.0
		if !nonIID {
			baseLight = 500.0
		}
		light := clamp(baseLight+rand.NormFloat64()*150.0, 50, 1000)

		targetTemp := 22.0
		if presence > 0.5 {
			targetTemp += 2.0
		}
		if hour >= 6 && hour <= 8 {
			targetTemp += 1.0 // morning warmth
		}
		if hour >= 22 || hour < 6 {
			targetTemp -= 3.0 // night setback
		}
		targetTemp = clamp(targetTemp, 18.0, 28.0)

		targetHum := 50.0
		if presence > 0.5 {
			targetHum = 55.0
		}
		targetHum = clamp(targetHum, 30.0, 70.0)

		targetLight := 100.0
		if presence > 0.5 {
			if hour >= 6 && hour <= 22 {
				targetLight = 350.0
			} else {
				targetLight = 50.0
			}
		}

		samples[i] = Sample{
			Features: []float64{
				(temp - 15) / 20,          // temp → [0,1]
				(hum - 20) / 60,           // humidity → [0,1]
				(light - 50) / 950,        // light → [0,1]
				presence,                  // already [0,1]
				hour / 23,                 // hour → [0,1]
			},
			Target: []float64{
				(targetTemp - 18) / 10,    // targetTemp → [0,1]
				(targetHum - 30) / 40,     // targetHum → [0,1]
				(targetLight - 50) / 300,  // targetLight → [0,1]
			},
			RoomID: roomID,
		}
	}
	return samples
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
