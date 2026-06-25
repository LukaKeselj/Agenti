package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/actor-framework/remote"
	"github.com/LukaKeselj/Agenti/smart-home/actors"
	"github.com/LukaKeselj/Agenti/smart-home/data"
)

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
	coordAddr := env("COORD_ADDR", "coordinator:50051")
	grpcAddr := env("SENSOR_ADDR", ":5051")
	sensorID := env("SENSOR_ID", "sensor-kitchen")
	roomID := env("ROOM_ID", "kitchen")
	samples := envInt("SAMPLES", 800)
	nonIID := env("NON_IID", "true") == "true"
	epochs := envInt("EPOCHS", 1)
	lr := 0.01
	if v := env("LEARNING_RATE", ""); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			lr = f
		}
	}
	coordID := env("COORD_ID", "coordinator")
	myAddr := env("MY_ADDR", "")

	fmt.Printf("Sensor %s (room: %s)\n", sensorID, roomID)
	fmt.Printf("  Coordinator  : %s\n", coordAddr)
	fmt.Printf("  My gRPC addr : %s\n", grpcAddr)
	fmt.Printf("  Samples      : %d\n", samples)
	fmt.Printf("  Epochs       : %d\n", epochs)
	fmt.Printf("  LR           : %.4f\n", lr)
	fmt.Printf("  My address   : %s\n", myAddr)
	fmt.Println()

	// Generate training data.
	datasets := data.GenerateSensors([]string{roomID}, samples, nonIID)
	trainData := datasets[roomID]

	sys := af.NewActorSystem(sensorID)
	defer sys.Shutdown()

	// Start gRPC server so coordinator can reach us.
	server := remote.NewActorServer(sys)
	if err := server.Start(grpcAddr); err != nil {
		panic(err)
	}
	fmt.Printf("gRPC server listening on %s\n", server.Addr())

	// Remote ref to coordinator.
	coordRef := remote.NewRemoteActorRef(af.ActorID(coordID), coordAddr)

	// Spawn sensor actor.
	sensor := actors.NewSensorActor(af.ActorID(sensorID), roomID, af.ActorID(coordID), trainData)
	sensor.SetCoordinatorRef(coordRef) // use remote ref for coordinator
	sys.MustSpawn(sensor, af.DefaultSpawnOptions())

	// Determine the address to advertise to the coordinator.
	// In Docker, MY_ADDR should be the service hostname:port (e.g. "sensor-kitchen:5051").
	// Locally, server.Addr() gives the actual listening address.
	advertiseAddr := myAddr
	if advertiseAddr == "" {
		advertiseAddr = server.Addr()
	}

	// Register with coordinator (include our gRPC address).
	// Give server a moment to start, then send registration.
	time.Sleep(500 * time.Millisecond)
	actors.RegisterSensor(coordRef, sensorID, roomID, len(trainData), advertiseAddr)
	fmt.Printf("Registered with coordinator at %s (advertising %s)\n\n", coordAddr, advertiseAddr)

	// Keep running – FL rounds are driven by the coordinator.
	select {}
}
