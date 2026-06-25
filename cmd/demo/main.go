package main

import (
	"fmt"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/actor-framework/supervision"
	"github.com/LukaKeselj/Agenti/smart-home/actors"
	"github.com/LukaKeselj/Agenti/smart-home/data"
	"github.com/LukaKeselj/Agenti/smart-home/evaluation"
	"github.com/LukaKeselj/Agenti/smart-home/model"
	"github.com/LukaKeselj/Agenti/smart-home/persistence"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║     Pametna kuća – Federated Learning Demo      ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Println()

	sys := af.NewActorSystem("smart-home-demo")
	defer sys.Shutdown()

	// ── Bootstrap ────────────────────────────────────────────
	opts := af.DefaultSpawnOptions()

	// Persistence.
	pers, err := persistence.NewPersister("./demo-data")
	if err != nil {
		panic(err)
	}
	fmt.Println("Persistence directory: ./demo-data")

	// Logger.
	logger := actors.NewLoggerActor("logger")
	sys.MustSpawn(logger, opts)
	fmt.Println("Logger actor spawned")

	// Device controller.
	devCtrl := actors.NewDeviceControllerActor("device-ctrl")
	sys.MustSpawn(devCtrl, opts)
	fmt.Println("Device controller spawned")

	// Supervisor.
	sup := supervision.NewSupervisorActor("supervisor", supervision.SupervisionConfig{
		Strategy:          &supervision.OneForOne{MaxRetries: 2, Within: 30 * time.Second},
		HeartbeatInterval: 0, // disabled for this demo
	})
	supRef := sys.MustSpawn(sup, opts)
	fmt.Println("Supervisor spawned")

	// Coordinator.
	coord := actors.NewCoordinatorActor("coordinator", "logger", "device-ctrl", 5*8+8+8*3+3)
	coord.SetTrainingConfig(1, 0.01)
	coordRef := sys.MustSpawn(coord, opts)
	fmt.Println("Coordinator spawned")

	// ── Data ─────────────────────────────────────────────────
	rooms := []string{"kitchen", "livingroom", "bedroom", "bathroom", "garage"}
	datasets := data.GenerateSensors(rooms, 800, true) // non-IID

	fmt.Printf("Generated %d rooms, %d samples/sensor\n", len(rooms), 800)

	// ── Sensors ──────────────────────────────────────────────
	for _, room := range rooms {
		sensorID := "sensor-" + room
		actors.RegisterSensor(coordRef, sensorID, room, len(datasets[room]))
		time.Sleep(50 * time.Millisecond)

		sensor := actors.NewSensorActor(af.ActorID(sensorID), room, "coordinator", datasets[room])
		_ = sys.MustSpawn(sensor, af.SpawnOptions{
			SupervisorID: "supervisor",
		})

		supervision.Register(supRef, af.ActorID(sensorID), func() af.Actor {
			return actors.NewSensorActor(af.ActorID(sensorID), room, "coordinator", datasets[room])
		}, af.SpawnOptions{
			SupervisorID: "supervisor",
		})
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Printf("Sensors registered and spawned\n\n")

	// ── FL Rounds ────────────────────────────────────────────
	numRounds := 10
	var results []evaluation.RoundResult

	// Holdout validation set (same for all rounds).
	valSet := data.GenerateSamples("validation", 100, 0.5, false)

	for round := 1; round <= numRounds; round++ {
		fmt.Printf("── Round %d ────────────────────────────────────\n", round)
		actors.StartRound(coordRef)

		// Wait for round to complete.
		time.Sleep(1 * time.Second)
		for {
			_, ok := logger.LastRound()
			if ok && len(logger.Rounds()) == round {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}

		// Evaluate global model on validation set.
		coordWeights := coord.Weights()
		evalMLP := model.NewMLP(5, 8, 3)
		evalMLP.SetWeights(coordWeights)

		var actuals, predictions []float64
		for _, s := range valSet {
			pred := evalMLP.Predict(s.Features)
			actuals = append(actuals, s.Target...)
			predictions = append(predictions, pred...)
		}

		metrics := evaluation.Calculate(actuals, predictions)
		results = append(results, evaluation.RoundResult{Round: round, Metrics: metrics})

		fmt.Printf("  Loss: %.6f | MSE: %.6f | RMSE: %.6f | R²: %.6f\n\n",
			logger.Rounds()[round-1].GlobalLoss, metrics.MSE, metrics.RMSE, metrics.R2)
	}

	// ── Convergence table ────────────────────────────────────
	evaluation.PrintConvergenceTable(results)
	if len(results) > 0 {
		evaluation.PrintSummary(len(results), results[len(results)-1].Metrics)
	}

	// ── Supervision demo ─────────────────────────────────────
	fmt.Println("── Supervision Demo ────────────────────────────")
	fmt.Println("Crashing sensor-kitchen...")

	kitchenRef, err := sys.Lookup("sensor-kitchen")
	if err != nil {
		fmt.Println("  sensor-kitchen not found (already crashed?)")
	} else {
		kitchenRef.Tell(af.Message{MsgType: "crash"})
		time.Sleep(1 * time.Second)

		newRef, err := sys.Lookup("sensor-kitchen")
		if err != nil {
			fmt.Println("  ✗ sensor-kitchen was NOT restarted!")
		} else {
			fmt.Println("  ✓ sensor-kitchen was restarted by supervisor")
			newRef.Tell(af.Message{MsgType: "ping"})
		}
	}
	fmt.Println()

	// ── Persistence demo ─────────────────────────────────────
	fmt.Println("── Persistence Demo ────────────────────────────")
	state := map[string]any{
		"rounds_completed": len(results),
		"final_rmse":       results[len(results)-1].Metrics.RMSE,
		"timestamp":        time.Now().Format(time.RFC3339),
	}
	if err := pers.Save("demo-state", state); err != nil {
		fmt.Printf("  ✗ Save failed: %v\n", err)
	} else {
		fmt.Println("  ✓ State saved to ./demo-data/demo-state.json")

		var loaded map[string]any
		if err := pers.Load("demo-state", &loaded); err != nil {
			fmt.Printf("  ✗ Load failed: %v\n", err)
		} else {
			fmt.Printf("  ✓ State loaded: %v\n", loaded)
		}
	}
	fmt.Println()

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║              Demo complete!                      ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
}
