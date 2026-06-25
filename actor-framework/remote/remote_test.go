package remote_test

import (
	"context"
	"sync"
	"testing"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/actor-framework/remote"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, srv.Start("127.0.0.1:0"))
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
		require.Failf(t, "timeout", "got %d messages", actor.Count())
	}

	require.Equal(t, 3, actor.Count())
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
	require.NoError(t, err)
	require.Equal(t, "world", reply.Payload)
}

func TestRemote_AskTimeout(t *testing.T) {
	sys := af.NewActorSystem("test-ask-timeout")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)
	sys.MustSpawn(newCountActor("sink", 999), af.DefaultSpawnOptions())

	ref := remote.NewRemoteActorRef("sink", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := ref.Ask(ctx, af.Message{MsgType: "req"})
	require.Error(t, err)
}

func TestRemote_Stop(t *testing.T) {
	sys := af.NewActorSystem("test-stop")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)
	sys.MustSpawn(newEchoActor("stoppable"), af.DefaultSpawnOptions())

	_, err := sys.Lookup("stoppable")
	require.NoError(t, err)

	ref := remote.NewRemoteActorRef("stoppable", addr)
	ref.Stop()

	time.Sleep(200 * time.Millisecond)

	_, err = sys.Lookup("stoppable")
	require.Error(t, err)
}

func TestRemote_TwoSystems(t *testing.T) {
	sysA := af.NewActorSystem("A")
	defer sysA.Shutdown()

	sysB := af.NewActorSystem("B")
	defer sysB.Shutdown()

	addrA := startTestServer(t, sysA)
	sysA.MustSpawn(newEchoActor("alice"), af.DefaultSpawnOptions())

	ref := remote.NewRemoteActorRef("alice", addrA)

	done := make(chan struct{}, 1)
	actorB := newCountActor("bob", 1)
	sysB.MustSpawn(actorB, af.DefaultSpawnOptions())
	_ = actorB

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := ref.Ask(ctx, af.Message{MsgType: "ping", Payload: "pong"})
	require.NoError(t, err)
	require.Equal(t, "pong", reply.Payload)
	_ = done
}

func TestRemote_UnknownActor(t *testing.T) {
	sys := af.NewActorSystem("test-unknown")
	defer sys.Shutdown()

	addr := startTestServer(t, sys)

	ref := remote.NewRemoteActorRef("ghost", addr)

	ref.Tell(af.Message{MsgType: "hello"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := ref.Ask(ctx, af.Message{MsgType: "req"})
	require.Error(t, err)
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
	require.NoError(t, err)

	got, ok := reply.Payload.(map[string]any)
	require.True(t, ok)
	require.Equal(t, 23.5, got["temp"])
	require.Equal(t, 60.0, got["hum"])
}
