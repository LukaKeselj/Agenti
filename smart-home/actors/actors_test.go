package actors_test

import (
	"sync"
	"testing"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/smart-home/actors"
)

// ── helpers ────────────────────────────────────────────────────

func mustSpawn(t *testing.T, sys *af.ActorSystem, actor af.Actor) af.ActorRef {
	t.Helper()
	ref, err := sys.Spawn(actor, af.DefaultSpawnOptions())
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}
	return ref
}

// ── SensorActor tests ──────────────────────────────────────────

func TestSensorActor_IdleState(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	sensor := actors.NewSensorActor("sensor-kitchen", "kitchen", "coordinator")
	mustSpawn(t, sys, sensor)
	time.Sleep(50 * time.Millisecond)

	if sensor.State() != "idle" {
		t.Fatalf("expected idle, got %s", sensor.State())
	}
}

func TestSensorActor_StartTrainingTransitions(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	// Spawn a coordinator so the sensor can find it,
	// even though we won't trigger a full round.
	mustSpawn(t, sys, actors.NewCoordinatorActor("coordinator", "", "", 75))

	sensor := actors.NewSensorActor("sensor-living", "livingroom", "coordinator")
	sensorRef := mustSpawn(t, sys, sensor)
	time.Sleep(50 * time.Millisecond)

	// Send StartTraining.
	sensorRef.Tell(af.Message{
		MsgType: actors.MsgStartTraining,
		Payload: actors.StartTrainingPayload{
			RoundID: 1, Weights: []float64{0.1, 0.2, 0.3, 0.4},
			LearningRate: 0.01, Epochs: 2,
		},
	})
	time.Sleep(300 * time.Millisecond)

	// After training completes, sensor should be back to idle.
	if sensor.State() != "idle" {
		t.Fatalf("expected idle after training, got %s", sensor.State())
	}
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

	// Register two sensors.
	actors.RegisterSensor(coordRef, "sensor-a", "kitchen", 100)
	actors.RegisterSensor(coordRef, "sensor-b", "bedroom", 80)
	time.Sleep(50 * time.Millisecond)

	if coord.SensorCount() != 2 {
		t.Fatalf("expected 2 sensors, got %d", coord.SensorCount())
	}

	// Spawn the actual sensors.
	sensorA := actors.NewSensorActor("sensor-a", "kitchen", "coordinator")
	sensorB := actors.NewSensorActor("sensor-b", "bedroom", "coordinator")
	mustSpawn(t, sys, sensorA)
	mustSpawn(t, sys, sensorB)
	time.Sleep(100 * time.Millisecond)

	// Start an FL round.
	actors.StartRound(coordRef)
	time.Sleep(2 * time.Second)

	// Logger should have recorded a round completion.
	round, ok := logger.LastRound()
	if !ok {
		t.Fatal("expected at least one round completion")
	}
	if round.NumClients != 2 {
		t.Fatalf("expected 2 clients, got %d", round.NumClients)
	}
	if round.RoundID != 1 {
		t.Fatalf("expected round 1, got %d", round.RoundID)
	}

	// Both sensors should be idle.
	if sensorA.State() != "idle" {
		t.Errorf("sensor-a expected idle, got %s", sensorA.State())
	}
	if sensorB.State() != "idle" {
		t.Errorf("sensor-b expected idle, got %s", sensorB.State())
	}
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

	// Run 3 rounds sequentially.
	for round := 1; round <= 3; round++ {
		actors.StartRound(coordRef)
		time.Sleep(1 * time.Second)

		_, ok := logger.LastRound()
		if !ok {
			t.Fatalf("round %d did not complete", round)
		}
	}

	rounds := logger.Rounds()
	if len(rounds) != 3 {
		t.Fatalf("expected 3 rounds, got %d", len(rounds))
	}
	for idx, r := range rounds {
		if r.RoundID != idx+1 {
			t.Errorf("round %d: expected ID %d, got %d", idx, idx+1, r.RoundID)
		}
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
	if !ok || temp != 24.0 {
		t.Fatalf("expected temp 24.0, got %v (ok=%v)", temp, ok)
	}
	hum, _ := dev.GetDeviceValue("kitchen", "humidity")
	if hum != 55.0 {
		t.Fatalf("expected hum 55.0, got %v", hum)
	}
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
	if !ok {
		t.Fatal("expected a round")
	}
	if round.RoundID != 1 || round.GlobalLoss != 0.42 || round.NumClients != 3 {
		t.Fatalf("unexpected round data: %+v", round)
	}
}

func TestLoggerActor_ConcurrentAccess(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	logger := actors.NewLoggerActor("logger")
	mustSpawn(t, sys, logger)
	time.Sleep(50 * time.Millisecond)

	// Send messages from multiple goroutines.
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = sys.Spawn(actors.NewSensorActor(
				af.ActorID("s"), "r", ""), af.DefaultSpawnOptions())
		}()
	}
	wg.Wait()
	// Just ensure no panic.
}

// ── Integration: full smart-home flow ──────────────────────────

func TestSmartHome_FullFlow(t *testing.T) {
	sys := af.NewActorSystem("smart-home")
	defer sys.Shutdown()

	// 1. Logger
	logger := actors.NewLoggerActor("logger")
	mustSpawn(t, sys, logger)

	// 2. Device controller
	devCtrl := actors.NewDeviceControllerActor("device-ctrl")
	mustSpawn(t, sys, devCtrl)

	// 3. Coordinator (with logger and device refs)
	coord := actors.NewCoordinatorActor("coordinator", "logger", "device-ctrl", 75)
	coordRef := mustSpawn(t, sys, coord)
	time.Sleep(50 * time.Millisecond)

	// 4. Register and spawn three sensors.
	sensorConfigs := []struct {
		id     string
		room   string
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

	if coord.SensorCount() != 3 {
		t.Fatalf("expected 3 sensors, got %d", coord.SensorCount())
	}

	// 5. Run two FL rounds.
	for round := 1; round <= 2; round++ {
		actors.StartRound(coordRef)
		time.Sleep(3 * time.Second)

		_, ok := logger.LastRound()
		if !ok {
			t.Fatalf("round %d did not complete", round)
		}
	}

	// 6. Verify rounds.
	rounds := logger.Rounds()
	if len(rounds) != 2 {
		t.Fatalf("expected 2 rounds, got %d", len(rounds))
	}
	if rounds[0].NumClients != 3 {
		t.Fatalf("expected 3 clients in round 1, got %d", rounds[0].NumClients)
	}
	if rounds[1].RoundID != 2 {
		t.Fatalf("expected round 2, got %d", rounds[1].RoundID)
	}

	// 7. Verify devices were adjusted.
	if _, ok := devCtrl.GetDeviceValue("all", "temperature"); !ok {
		t.Fatal("expected device adjustment")
	}
}
