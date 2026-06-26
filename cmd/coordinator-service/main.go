package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/actor-framework/remote"
	"github.com/LukaKeselj/Agenti/smart-home/actors"
	"github.com/LukaKeselj/Agenti/smart-home/data"
	"github.com/LukaKeselj/Agenti/smart-home/evaluation"
	"github.com/LukaKeselj/Agenti/smart-home/model"
)

const numWeights = 5*8 + 8 + 8*3 + 3

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║     Pametna kuća – Coordinator Service          ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Println()

	grpcAddr := env("COORD_ADDR", ":50051")
	numRounds := envInt("NUM_ROUNDS", 5)
	epochs := envInt("EPOCHS", 1)
	lr := 0.01
	if v := env("LEARNING_RATE", ""); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			lr = f
		}
	}
	sensorAddrs := env("SENSOR_ADDRS", "")
	roomsStr := env("ROOMS", "kitchen,livingroom,bedroom,bathroom,garage")
	samplesPerSensor := envInt("SAMPLES_PER_SENSOR", 800)
	valSamples := envInt("VAL_SAMPLES", 100)

	rooms := strings.Split(roomsStr, ",")
	fmt.Printf("Coordinator gRPC address : %s\n", grpcAddr)
	fmt.Printf("FL rounds                : %d\n", numRounds)
	fmt.Printf("Epochs per round         : %d\n", epochs)
	fmt.Printf("Learning rate            : %.4f\n", lr)
	fmt.Printf("Rooms                    : %v\n", rooms)
	fmt.Println()

	sys := af.NewActorSystem("coordinator")

	opts := af.DefaultSpawnOptions()

	logger := actors.NewLoggerActor("logger")
	sys.MustSpawn(logger, opts)

	devCtrl := actors.NewDeviceControllerActor("device-ctrl")
	sys.MustSpawn(devCtrl, opts)

	coord := actors.NewCoordinatorActor("coordinator", "logger", "device-ctrl", numWeights)
	coord.SetTrainingConfig(epochs, lr)
	coordRef := sys.MustSpawn(coord, opts)

	// Explicitly register payload types for remote transport.
	remote.RegisterPayloadType(actors.StartTrainingPayload{})
	remote.RegisterPayloadType(actors.ModelUpdatePayload{})
	remote.RegisterPayloadType(actors.GlobalModelUpdatePayload{})
	remote.RegisterPayloadType(actors.RoundCompletePayload{})
	remote.RegisterPayloadType(actors.AdjustEnvironmentPayload{})
	remote.RegisterPayloadType(actors.DeviceStatusPayload{})
	remote.RegisterPayloadType(actors.LogMetricsPayload{})
	remote.RegisterPayloadType(actors.LogEventPayload{})
	remote.RegisterPayloadType(actors.RegisterSensorPayload{})
	remote.RegisterPayloadType(actors.StartRoundPayload{})
	remote.RegisterPayloadType(actors.TrainingCompletePayload{})
	remote.RegisterPayloadType(actors.RequestStatusPayload{})
	remote.RegisterPayloadType(actors.StatusResponsePayload{})
	remote.RegisterPayloadType(actors.EvaluateModelPayload{})
	remote.RegisterPayloadType(actors.EvaluationResultPayload{})
	fmt.Println("Registered payload types for remote transport")

	// Set callback for remote sensor registrations.
	coord.OnRemoteSensor(func(sensorID, addr string) {
		ref := remote.NewRemoteActorRef(af.ActorID(sensorID), addr)
		coord.SetSensorRemoteRef(sensorID, ref)
	})

	// Start gRPC server so sensors can register remotely.
	server := remote.NewActorServer(sys)
	if err := server.Start(grpcAddr); err != nil {
		panic(err)
	}
	fmt.Printf("gRPC server listening on %s\n\n", server.Addr())

	// Generate validation set.
	valSet := data.GenerateSamples("validation", valSamples, 0.5, false)
	evaluator := actors.NewEvaluatorActor("evaluator", valSet)
	evalRef := sys.MustSpawn(evaluator, opts)
	fmt.Println("Evaluator actor spawned")

	// Parse optional sensor addresses from env.
	// Format: "sensor-kitchen=kitchen:5051,sensor-livingroom=livingroom:5052,..."
	manualSensors := make(map[string]string) // sensorID → address
	if sensorAddrs != "" {
		for _, part := range strings.Split(sensorAddrs, ",") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				manualSensors[kv[0]] = kv[1]
			}
		}
	}

	// If sensors were specified manually (not via gRPC registration), pre-register.
	if len(manualSensors) > 0 {
		fmt.Println("Registering sensors from SENSOR_ADDRS...")
		for sensorID, addr := range manualSensors {
			ref := remote.NewRemoteActorRef(af.ActorID(sensorID), addr)
			coord.SetSensorRemoteRef(sensorID, ref)
			actors.RegisterSensor(coordRef, sensorID, strings.TrimPrefix(sensorID, "sensor-"), samplesPerSensor)
			fmt.Printf("  %s → %s\n", sensorID, addr)
		}
		fmt.Println()
	} else {
		minSensors := envInt("MIN_SENSORS", 1)
		fmt.Printf("Waiting for sensors to register via gRPC (need ≥%d)...\n", minSensors)
		deadline := time.Now().Add(30 * time.Second)
		for {
			if coord.SensorCount() >= minSensors {
				break
			}
			if time.Now().After(deadline) {
				fmt.Printf("  Warning: only %d sensors registered after 30s (need %d)\n",
					coord.SensorCount(), minSensors)
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Printf("Registered sensors: %d\n\n", coord.SensorCount())
	}

	// ── FL Rounds ────────────────────────────────────────────
	var results []evaluation.RoundResult

	for round := 1; round <= numRounds; round++ {
		fmt.Printf("── Round %d ────────────────────────────────────\n", round)

		actors.StartRound(coordRef)

		time.Sleep(1 * time.Second)
		roundDeadline := time.Now().Add(30 * time.Second)
		roundCompleted := false
		for time.Now().Before(roundDeadline) {
			if rd, ok := logger.LastRound(); ok && rd.RoundID == round {
				roundCompleted = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !roundCompleted {
			fmt.Printf("  ⚠ Round %d did not complete within timeout\n\n", round)
			continue
		}

		coordWeights := coord.Weights()

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
			lastRound, _ := logger.LastRound()
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

		lastRound, _ := logger.LastRound()
		fmt.Printf("  Loss: %.6f | MSE: %.6f | RMSE: %.6f | R²: %.6f\n\n",
			lastRound.GlobalLoss, metrics.MSE, metrics.RMSE, metrics.R2)
	}

	// ── Results ──────────────────────────────────────────────
	evaluation.PrintConvergenceTable(results)
	if len(results) > 0 {
		evaluation.PrintSummary(len(results), results[len(results)-1].Metrics)
	}

	svgPath := "convergence.svg"
	if err := evaluation.GenerateSVG(svgPath, results); err != nil {
		fmt.Printf("  ✗ SVG chart failed: %v\n", err)
	} else {
		fmt.Printf("  ✓ SVG chart saved to %s\n", svgPath)
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║            Coordinator complete!                 ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	os.Stdout.Sync() // flush stdout before slog (stderr) shutdown messages
	sys.Shutdown()
}
