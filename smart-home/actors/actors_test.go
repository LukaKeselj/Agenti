package actors_test

import (
	"sync"
	"testing"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/smart-home/actors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── helpers ────────────────────────────────────────────────────

func mustSpawn(t *testing.T, sys *af.ActorSystem, actor af.Actor) af.ActorRef {
	t.Helper()
	ref, err := sys.Spawn(actor, af.DefaultSpawnOptions())
	require.NoError(t, err)
	return ref
}

// ── SensorActor tests ──────────────────────────────────────────

func TestSensorActor_IdleState(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	sensor := actors.NewSensorActor("sensor-kitchen", "kitchen", "coordinator")
	mustSpawn(t, sys, sensor)
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, "idle", sensor.State())
}

func TestSensorActor_StartTrainingTransitions(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	mustSpawn(t, sys, actors.NewCoordinatorActor("coordinator", "", "", 75))

	sensor := actors.NewSensorActor("sensor-living", "livingroom", "coordinator")
	sensorRef := mustSpawn(t, sys, sensor)
	time.Sleep(50 * time.Millisecond)

	sensorRef.Tell(af.Message{
		MsgType: actors.MsgStartTraining,
		Payload: actors.StartTrainingPayload{
			RoundID: 1, Weights: []float64{0.1, 0.2, 0.3, 0.4},
			LearningRate: 0.01, Epochs: 2,
		},
	})
	time.Sleep(300 * time.Millisecond)

	require.Equal(t, "idle", sensor.State())
}

// ── CoordinatorActor tests ─────────────────────────────────────

func TestCoordinator_FullRound(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	logger := actors.NewLoggerActor("logger")
	mustSpawn(t, sys, logger)

	coord := actors.NewCoordinatorActor("coordinator", "logger", "", 75)
	coordRef := mustSpawn(t, sys, coord)
	time.Sleep(50 * time.Millisecond)

	actors.RegisterSensor(coordRef, "sensor-a", "kitchen", 100)
	actors.RegisterSensor(coordRef, "sensor-b", "bedroom", 80)
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, 2, coord.SensorCount())

	sensorA := actors.NewSensorActor("sensor-a", "kitchen", "coordinator")
	sensorB := actors.NewSensorActor("sensor-b", "bedroom", "coordinator")
	mustSpawn(t, sys, sensorA)
	mustSpawn(t, sys, sensorB)
	time.Sleep(100 * time.Millisecond)

	actors.StartRound(coordRef)
	time.Sleep(2 * time.Second)

	round, ok := logger.LastRound()
	require.True(t, ok)
	require.Equal(t, 2, round.NumClients)
	require.Equal(t, 1, round.RoundID)

	assert.Equal(t, "idle", sensorA.State())
	assert.Equal(t, "idle", sensorB.State())
}

func TestCoordinator_MultipleRounds(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	logger := actors.NewLoggerActor("logger")
	mustSpawn(t, sys, logger)

	coord := actors.NewCoordinatorActor("coordinator", "logger", "", 75)
	coordRef := mustSpawn(t, sys, coord)
	time.Sleep(50 * time.Millisecond)

	actors.RegisterSensor(coordRef, "multi-sensor", "garage", 60)
	time.Sleep(50 * time.Millisecond)

	sensor := actors.NewSensorActor("multi-sensor", "garage", "coordinator")
	mustSpawn(t, sys, sensor)
	time.Sleep(100 * time.Millisecond)

	for round := 1; round <= 3; round++ {
		actors.StartRound(coordRef)
		time.Sleep(1 * time.Second)

		_, ok := logger.LastRound()
		require.True(t, ok, "round %d did not complete", round)
	}

	rounds := logger.Rounds()
	require.Len(t, rounds, 3)
	for idx, r := range rounds {
		assert.Equal(t, idx+1, r.RoundID, "round %d", idx)
	}
}

// ── DeviceControllerActor tests ────────────────────────────────

func TestDeviceController_AdjustEnvironment(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	dev := actors.NewDeviceControllerActor("device-ctrl")
	devRef := mustSpawn(t, sys, dev)
	time.Sleep(50 * time.Millisecond)

	devRef.Tell(af.Message{
		MsgType: actors.MsgAdjustEnvironment,
		Payload: actors.AdjustEnvironmentPayload{
			RoomID: "kitchen", TargetTemp: 24.0, TargetHum: 55.0, TargetLight: 400.0,
		},
	})
	time.Sleep(100 * time.Millisecond)

	temp, ok := dev.GetDeviceValue("kitchen", "temperature")
	require.True(t, ok)
	require.Equal(t, 24.0, temp)
	hum, _ := dev.GetDeviceValue("kitchen", "humidity")
	require.Equal(t, 55.0, hum)
}

// ── LoggerActor tests ──────────────────────────────────────────

func TestLoggerActor_RecordsRound(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	logger := actors.NewLoggerActor("logger")
	loggerRef := mustSpawn(t, sys, logger)
	time.Sleep(50 * time.Millisecond)

	loggerRef.Tell(af.Message{
		MsgType: actors.MsgRoundComplete,
		Payload: actors.RoundCompletePayload{RoundID: 1, GlobalLoss: 0.42, NumClients: 3, ElapsedMs: 150},
	})
	time.Sleep(50 * time.Millisecond)

	round, ok := logger.LastRound()
	require.True(t, ok)
	require.Equal(t, 1, round.RoundID)
	require.Equal(t, 0.42, round.GlobalLoss)
	require.Equal(t, 3, round.NumClients)
}

func TestLoggerActor_ConcurrentMessages(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	logger := actors.NewLoggerActor("logger")
	logRef := mustSpawn(t, sys, logger)
	time.Sleep(50 * time.Millisecond)

	// Send messages to the logger from multiple goroutines concurrently.
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(round int) {
			defer wg.Done()
			logRef.Tell(af.Message{
				MsgType: actors.MsgRoundComplete,
				Payload: actors.RoundCompletePayload{RoundID: round, NumClients: 1},
			})
		}(i + 1)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	// All 20 messages should have been recorded without a data race.
	require.Len(t, logger.Rounds(), 20)
}

// ── Integration: full smart-home flow ──────────────────────────

func TestSmartHome_FullFlow(t *testing.T) {
	sys := af.NewActorSystem("smart-home")
	defer sys.Shutdown()

	logger := actors.NewLoggerActor("logger")
	mustSpawn(t, sys, logger)

	devCtrl := actors.NewDeviceControllerActor("device-ctrl")
	mustSpawn(t, sys, devCtrl)

	coord := actors.NewCoordinatorActor("coordinator", "logger", "device-ctrl", 75)
	coordRef := mustSpawn(t, sys, coord)
	time.Sleep(50 * time.Millisecond)

	sensorConfigs := []struct {
		id      string
		room    string
		samples int
	}{
		{"sensor-kitchen", "kitchen", 100},
		{"sensor-living", "livingroom", 80},
		{"sensor-bedroom", "bedroom", 60},
	}
	for _, sc := range sensorConfigs {
		actors.RegisterSensor(coordRef, sc.id, sc.room, sc.samples)
		mustSpawn(t, sys, actors.NewSensorActor(
			af.ActorID(sc.id), sc.room, "coordinator"))
	}
	time.Sleep(100 * time.Millisecond)

	require.Equal(t, 3, coord.SensorCount())

	for round := 1; round <= 2; round++ {
		actors.StartRound(coordRef)
		time.Sleep(3 * time.Second)

		_, ok := logger.LastRound()
		require.True(t, ok, "round %d did not complete", round)
	}

	rounds := logger.Rounds()
	require.Len(t, rounds, 2)
	require.Equal(t, 3, rounds[0].NumClients)
	require.Equal(t, 2, rounds[1].RoundID)

	_, ok := devCtrl.GetDeviceValue("all", "temperature")
	require.True(t, ok)
}
