package main

import (
	"context"
	"fmt"
	"os"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/actor-framework/supervision"
	"github.com/LukaKeselj/Agenti/smart-home/actors"
	"github.com/LukaKeselj/Agenti/smart-home/data"
	"github.com/LukaKeselj/Agenti/smart-home/evaluation"
	"github.com/LukaKeselj/Agenti/smart-home/model"
	"github.com/LukaKeselj/Agenti/smart-home/persistence"
	"gopkg.in/yaml.v3"
)

type DemoConfig struct {
	Demo struct {
		NumRounds            int    `yaml:"num_rounds"`
		PersistenceDir       string `yaml:"persistence_dir"`
		HeartbeatIntervalSec int    `yaml:"heartbeat_interval_sec"`
	} `yaml:"demo"`
	Training struct {
		Epochs       int     `yaml:"epochs"`
		LearningRate float64 `yaml:"learning_rate"`
	} `yaml:"training"`
	Sensors struct {
		Rooms            []string `yaml:"rooms"`
		SamplesPerSensor int      `yaml:"samples_per_sensor"`
		NonIID           bool     `yaml:"non_iid"`
	} `yaml:"sensors"`
	Supervision struct {
		Strategy    string `yaml:"strategy"`
		MaxRetries  int    `yaml:"max_retries"`
		WithinSec   int    `yaml:"within_sec"`
	} `yaml:"supervision"`
	Validation struct {
		Samples int `yaml:"samples"`
	} `yaml:"validation"`
}

func defaultConfig() DemoConfig {
	var cfg DemoConfig
	cfg.Demo.NumRounds = 10
	cfg.Demo.PersistenceDir = "./demo-data"
	cfg.Demo.HeartbeatIntervalSec = 5
	cfg.Training.Epochs = 1
	cfg.Training.LearningRate = 0.01
	cfg.Sensors.Rooms = []string{"kitchen", "livingroom", "bedroom", "bathroom", "garage"}
	cfg.Sensors.SamplesPerSensor = 800
	cfg.Sensors.NonIID = true
	cfg.Supervision.MaxRetries = 3
	cfg.Supervision.WithinSec = 30
	cfg.Validation.Samples = 100
	return cfg
}

func loadConfig(path string) (DemoConfig, error) {
	cfg := defaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func main() {
	// ── Config ────────────────────────────────────────────────
	configPath := os.Getenv("DEMO_CONFIG")
	if configPath == "" {
		configPath = "./demo/config.yaml"
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		fmt.Printf("Config load error (%s), using defaults: %v\n", configPath, err)
		cfg = defaultConfig()
	}

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║     Pametna kuća – Federated Learning Demo      ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Println()

	sys := af.NewActorSystem("smart-home-demo")
	defer sys.Shutdown()

	// ── Bootstrap ────────────────────────────────────────────
	opts := af.DefaultSpawnOptions()

	pers, err := persistence.NewPersister(cfg.Demo.PersistenceDir)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Persistence directory: %s\n", cfg.Demo.PersistenceDir)

	logger := actors.NewLoggerActor("logger")
	logger.SetPersister(pers)
	sys.MustSpawn(logger, opts)
	fmt.Println("Logger actor spawned")

	devCtrl := actors.NewDeviceControllerActor("device-ctrl")
	devCtrl.SetPersister(pers)
	sys.MustSpawn(devCtrl, opts)
	fmt.Println("Device controller spawned")

	sup := supervision.NewSupervisorActor("supervisor", supervision.SupervisionConfig{
		Strategy:          &supervision.OneForOne{MaxRetries: cfg.Supervision.MaxRetries, Within: time.Duration(cfg.Supervision.WithinSec) * time.Second},
		HeartbeatInterval: time.Duration(cfg.Demo.HeartbeatIntervalSec) * time.Second,
		LoggerID:          "logger",
	})
	supRef := sys.MustSpawn(sup, opts)
	fmt.Println("Supervisor spawned")

	coord := actors.NewCoordinatorActor("coordinator", "logger", "device-ctrl", 5*8+8+8*3+3)
	coord.SetTrainingConfig(cfg.Training.Epochs, cfg.Training.LearningRate)
	coord.SetPersister(pers)
	coordRef := sys.MustSpawn(coord, opts)
	fmt.Println("Coordinator spawned")

	// ── Data ─────────────────────────────────────────────────
	rooms := cfg.Sensors.Rooms
	datasets := data.GenerateSensors(rooms, cfg.Sensors.SamplesPerSensor, cfg.Sensors.NonIID)

	fmt.Printf("Generated %d rooms, %d samples/sensor\n", len(rooms), cfg.Sensors.SamplesPerSensor)

	// ── Sensors ──────────────────────────────────────────────
	for _, room := range rooms {
		sensorID := "sensor-" + room
		actors.RegisterSensor(coordRef, sensorID, room, len(datasets[room]))
		time.Sleep(50 * time.Millisecond)

		sensor := actors.NewSensorActor(af.ActorID(sensorID), room, "coordinator", datasets[room])
		sensor.SetPersister(pers)
		_ = sys.MustSpawn(sensor, af.SpawnOptions{
			SupervisorID: "supervisor",
		})

		supervision.Register(supRef, af.ActorID(sensorID), func() af.Actor {
			s := actors.NewSensorActor(af.ActorID(sensorID), room, "coordinator", datasets[room])
			s.SetPersister(pers) // restore persisted state on supervisor restart
			return s
		}, af.SpawnOptions{
			SupervisorID: "supervisor",
		})
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Printf("Sensors registered and spawned\n\n")

	// ── FL Rounds ────────────────────────────────────────────
	numRounds := cfg.Demo.NumRounds
	var results []evaluation.RoundResult

	valSet := data.GenerateSamples("validation", cfg.Validation.Samples, 0.5, false)
	evaluator := actors.NewEvaluatorActor("evaluator", valSet)
	evalRef := sys.MustSpawn(evaluator, opts)
	fmt.Println("Evaluator actor spawned")

	crashRound := 3
	if numRounds >= crashRound {
		fmt.Println("── Supervision demo: sensor will crash during round", crashRound, "──")
	}

	for round := 1; round <= numRounds; round++ {
		fmt.Printf("── Round %d ────────────────────────────────────\n", round)

		if round == crashRound && numRounds >= crashRound {
			actors.StartRound(coordRef)
			time.Sleep(1 * time.Millisecond)
			fmt.Println("  Crashing sensor-kitchen during active round...")
			if ref, err := sys.Lookup("sensor-kitchen"); err == nil {
				ref.Tell(af.Message{MsgType: "crash"})
				time.Sleep(500 * time.Millisecond)
				if _, err := sys.Lookup("sensor-kitchen"); err == nil {
					fmt.Println("  ✓ sensor-kitchen was restarted by supervisor")
				} else {
					fmt.Println("  ✗ sensor-kitchen was NOT restarted!")
				}
			}
		} else {
			actors.StartRound(coordRef)
		}

		// Wait for round completion with a timeout so a sensor crash can't hang the demo.
		time.Sleep(1 * time.Second)
		roundDeadline := time.Now().Add(15 * time.Second)
		roundCompleted := false
		for time.Now().Before(roundDeadline) {
			if rd, ok := logger.LastRound(); ok && rd.RoundID == round {
				roundCompleted = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !roundCompleted {
			fmt.Printf("  ⚠ Round %d did not complete within timeout (sensor may have crashed)\n\n", round)
			continue
		}

		coordWeights := coord.Weights()
		lastRound, _ := logger.LastRound()

		// Ask EvaluatorActor for metrics.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		reply, err := evalRef.Ask(ctx, af.Message{
			MsgType: actors.MsgEvaluateModel,
			Payload: actors.EvaluateModelPayload{
				RoundID:       round,
				GlobalWeights: coordWeights,
				LoggerID:      "logger",
			},
		})
		cancel()
		if err != nil {
			// Fallback: compute inline.
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
				lastRound.GlobalLoss, metrics.MSE, metrics.RMSE, metrics.R2)
			continue
		}

		res, ok := reply.Payload.(actors.EvaluationResultPayload)
		if !ok {
			fmt.Printf("  Unexpected reply type from evaluator\n")
			continue
		}
		metrics := evaluation.Metrics{MSE: res.MSE, RMSE: res.RMSE, MAE: res.MAE, R2: res.R2Score}
		results = append(results, evaluation.RoundResult{Round: round, Metrics: metrics})

		fmt.Printf("  Loss: %.6f | MSE: %.6f | RMSE: %.6f | R²: %.6f\n\n",
			lastRound.GlobalLoss, metrics.MSE, metrics.RMSE, metrics.R2)
	}

	// ── Convergence table ────────────────────────────────────
	evaluation.PrintConvergenceTable(results)
	if len(results) > 0 {
		evaluation.PrintSummary(len(results), results[len(results)-1].Metrics)
	}

	// ── SVG chart ────────────────────────────────────────────
	svgPath := "convergence.svg"
	if err := evaluation.GenerateSVG(svgPath, results); err != nil {
		fmt.Printf("  ✗ SVG chart failed: %v\n", err)
	} else {
		fmt.Printf("  ✓ SVG chart saved to %s\n", svgPath)
	}
	fmt.Println()

	// ── Persistence demo ─────────────────────────────────────
	fmt.Println("── Persistence Demo ────────────────────────────")
	state := map[string]any{
		"rounds_completed": len(results),
		"final_mse":        results[len(results)-1].Metrics.MSE,
		"final_rmse":       results[len(results)-1].Metrics.RMSE,
		"final_mae":        results[len(results)-1].Metrics.MAE,
		"final_r2":         results[len(results)-1].Metrics.R2,
		"timestamp":        time.Now().Format(time.RFC3339),
		"config":           configPath,
	}
	if err := pers.Save("demo-state", state); err != nil {
		fmt.Printf("  ✗ Save failed: %v\n", err)
	} else {
		fmt.Printf("  ✓ State saved to %s/demo-state.json\n", cfg.Demo.PersistenceDir)

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
