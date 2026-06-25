package remote_test

import (
	"context"
	"sync"
	"testing"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/actor-framework/remote"
)

// ── test actors ────────────────────────────────────────────────

type echoActor struct {
	af.BaseActor
}

func newEchoActor(id af.ActorID) *echoActor {
	return &echoActor{BaseActor: af.NewBaseActor(id)}
}

func (a *echoActor) Receive(_ af.ActorContext, msg af.Message) {
	if env, ok := af.IsAsk(msg); ok {
		env.ReplyCh <- af.Message{
			MsgType: msg.MsgType,
			Payload: env.Original.Payload,
		}
	}
}

type countActor struct {
	af.BaseActor
	mu    sync.Mutex
	count int
	done  chan struct{}
	want  int
}

func newCountActor(id af.ActorID, want int) *countActor {
	return &countActor{
		BaseActor: af.NewBaseActor(id),
		done:      make(chan struct{}),
		want:      want,
	}
}

func (a *countActor) Receive(_ af.ActorContext, msg af.Message) {
	if msg.MsgType == af.MsgStarted {
		return
	}
	a.mu.Lock()
	a.count++
	if a.count >= a.want {
		select {
		case <-a.done:
		default:
			close(a.done)
		}
	}
	a.mu.Unlock()
}

func (a *countActor) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.count
}

// ── helpers ────────────────────────────────────────────────────

func startTestServer(t *testing.T, sys *af.ActorSystem) string {
	t.Helper()
	srv := remote.NewActorServer(sys)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start gRPC server: %v", err)
	}
	t.Cleanup(srv.Shutdown)
	return srv.Addr()
}

// ── tests ──────────────────────────────────────────────────────

func TestRemote_Tell(t *testing.T) {
	sys := af.NewActorSystem("test-tell")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)
	actor := newCountActor("counter", 3)
	sys.MustSpawn(actor, af.DefaultSpawnOptions())

	ref := remote.NewRemoteActorRef("counter", addr)
	for i := 0; i < 3; i++ {
		ref.Tell(af.Message{MsgType: "ping"})
	}

	select {
	case <-actor.done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out; got %d messages", actor.Count())
	}

	if actor.Count() != 3 {
		t.Fatalf("expected 3, got %d", actor.Count())
	}
}

func TestRemote_Ask(t *testing.T) {
	sys := af.NewActorSystem("test-ask")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)
	sys.MustSpawn(newEchoActor("echo"), af.DefaultSpawnOptions())

	ref := remote.NewRemoteActorRef("echo", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := ref.Ask(ctx, af.Message{MsgType: "hello", Payload: "world"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Payload != "world" {
		t.Fatalf("expected 'world', got %v", reply.Payload)
	}
}

func TestRemote_AskTimeout(t *testing.T) {
	sys := af.NewActorSystem("test-ask-timeout")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)
	// countActor never replies to Ask → should timeout
	sys.MustSpawn(newCountActor("sink", 999), af.DefaultSpawnOptions())

	ref := remote.NewRemoteActorRef("sink", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := ref.Ask(ctx, af.Message{MsgType: "req"})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestRemote_Stop(t *testing.T) {
	sys := af.NewActorSystem("test-stop")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)
	sys.MustSpawn(newEchoActor("stoppable"), af.DefaultSpawnOptions())

	// Verify the actor exists.
	if _, err := sys.Lookup("stoppable"); err != nil {
		t.Fatal("actor should exist before Stop")
	}

	ref := remote.NewRemoteActorRef("stoppable", addr)
	ref.Stop()

	time.Sleep(200 * time.Millisecond)

	// After Stop, the actor should no longer be registered.
	if _, err := sys.Lookup("stoppable"); err == nil {
		t.Fatal("actor should have been removed after Stop")
	}
}

func TestRemote_TwoSystems(t *testing.T) {
	// System A hosts an actor; system B talks to it via RemoteActorRef.
	sysA := af.NewActorSystem("A")
	defer sysA.Shutdown()

	sysB := af.NewActorSystem("B")
	defer sysB.Shutdown()

	addrA := startTestServer(t, sysA)
	sysA.MustSpawn(newEchoActor("alice"), af.DefaultSpawnOptions())

	// From B's perspective, alice is remote.
	ref := remote.NewRemoteActorRef("alice", addrA)

	// Tell
	done := make(chan struct{}, 1)
	actorB := newCountActor("bob", 1)
	sysB.MustSpawn(actorB, af.DefaultSpawnOptions())
	_ = actorB // not used in this test, just for illustration

	// Ask – B talks to A's echo actor.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := ref.Ask(ctx, af.Message{MsgType: "ping", Payload: "pong"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Payload != "pong" {
		t.Fatalf("expected 'pong', got %v", reply.Payload)
	}
	_ = done
}

func TestRemote_UnknownActor(t *testing.T) {
	sys := af.NewActorSystem("test-unknown")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)

	// Point to a non-existent actor.
	ref := remote.NewRemoteActorRef("ghost", addr)

	// Tell should not panic.
	ref.Tell(af.Message{MsgType: "hello"})

	// Ask should return an error.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := ref.Ask(ctx, af.Message{MsgType: "req"})
	if err == nil {
		t.Fatal("expected error for unknown actor")
	}
}

func TestRemote_StructuredPayload(t *testing.T) {
	type sensorReading struct {
		Temp float64 `json:"temp"`
		Hum  float64 `json:"hum"`
	}

	sys := af.NewActorSystem("test-payload")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)
	sys.MustSpawn(newEchoActor("sensor"), af.DefaultSpawnOptions())

	ref := remote.NewRemoteActorRef("sensor", addr)

	reading := sensorReading{Temp: 23.5, Hum: 60.0}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := ref.Ask(ctx, af.Message{MsgType: "reading", Payload: reading})
	if err != nil {
		t.Fatal(err)
	}

	// The reply payload should be decodeable back to the original struct.
	// Since it goes through JSON, it'll come back as map[string]any.
	got, ok := reply.Payload.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", reply.Payload)
	}
	if got["temp"] != 23.5 {
		t.Errorf("expected temp 23.5, got %v", got["temp"])
	}
	if got["hum"] != 60.0 {
		t.Errorf("expected hum 60.0, got %v", got["hum"])
	}
}
