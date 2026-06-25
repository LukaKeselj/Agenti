package supervision_test

import (
	"context"
	"testing"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/actor-framework/supervision"
	"github.com/stretchr/testify/require"
)

// ── test helpers ───────────────────────────────────────────────

// countActor counts messages it receives (ignores system lifecycle).
type countActor struct {
	af.BaseActor
	ch chan int
}

func newCountActor(id af.ActorID, ch chan int) *countActor {
	return &countActor{BaseActor: af.NewBaseActor(id), ch: ch}
}

func (a *countActor) Receive(_ af.ActorContext, msg af.Message) {
	if msg.MsgType == af.MsgStarted {
		return
	}
	select {
	case a.ch <- 1:
	default:
	}
}

// crashActor panics when it receives a "crash" message.
type crashActor struct {
	af.BaseActor
}

func newCrashActor(id af.ActorID) *crashActor {
	return &crashActor{BaseActor: af.NewBaseActor(id)}
}

func (a *crashActor) Receive(_ af.ActorContext, msg af.Message) {
	if msg.MsgType == "crash" {
		panic("simulated crash for testing")
	}
}

// ── strategy unit tests ────────────────────────────────────────

func TestStrategy_OneForOne_Restart(t *testing.T) {
	strat := &supervision.OneForOne{MaxRetries: 3, Within: 10 * time.Second}
	d := strat.OnFailure("a1", []time.Time{time.Now()})
	require.Equal(t, supervision.ActionRestart, d.Action)
}

func TestStrategy_OneForOne_Escalate(t *testing.T) {
	now := time.Now()
	failures := make([]time.Time, 4)
	for i := range failures {
		failures[i] = now
	}
	strat := &supervision.OneForOne{MaxRetries: 3, Within: 10 * time.Second}
	d := strat.OnFailure("a1", failures)
	require.Equal(t, supervision.ActionEscalate, d.Action)
}

func TestStrategy_ExponentialBackoff_Delays(t *testing.T) {
	strat := &supervision.ExponentialBackoff{
		InitialDelay: time.Second,
		MaxDelay:     30 * time.Second,
		Factor:       2.0,
	}
	d1 := strat.OnFailure("a1", []time.Time{time.Now()})
	require.Equal(t, supervision.ActionRestart, d1.Action)
	require.Equal(t, time.Second, d1.Delay)

	d2 := strat.OnFailure("a1", []time.Time{time.Now(), time.Now()})
	require.Equal(t, 2*time.Second, d2.Delay)

	d6 := strat.OnFailure("a1", make([]time.Time, 6))
	require.Equal(t, 30*time.Second, d6.Delay)
}

func TestStrategy_Escalation(t *testing.T) {
	strat := &supervision.Escalation{}
	d := strat.OnFailure("a1", []time.Time{time.Now()})
	require.Equal(t, supervision.ActionEscalate, d.Action)
}

// ── supervisor integration tests ───────────────────────────────

func TestSupervisor_RestartOnFailure(t *testing.T) {
	sys := af.NewActorSystem("test-supervisor")
	defer sys.Shutdown()

	supRef, err := sys.Spawn(
		supervision.NewSupervisorActor("sup", supervision.SupervisionConfig{
			Strategy: &supervision.OneForOne{MaxRetries: 3, Within: 10 * time.Second},
		}),
		af.DefaultSpawnOptions(),
	)
	require.NoError(t, err)

	supervision.Register(supRef, "crasher", func() af.Actor {
		return newCrashActor("crasher")
	}, af.SpawnOptions{
		SupervisorID: "sup",
	})
	time.Sleep(50 * time.Millisecond)

	crasherRef, err := sys.Spawn(newCrashActor("crasher"), af.SpawnOptions{
		SupervisorID: "sup",
	})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	crasherRef.Tell(af.Message{MsgType: "crash"})
	time.Sleep(500 * time.Millisecond)

	newRef, err := sys.Lookup("crasher")
	require.NoError(t, err)
	newRef.Tell(af.Message{MsgType: "ping"})
}

func TestSupervisor_RegisterAndUnregister(t *testing.T) {
	sys := af.NewActorSystem("test-register")
	defer sys.Shutdown()

	supRef, _ := sys.Spawn(
		supervision.NewSupervisorActor("sup2", supervision.SupervisionConfig{
			Strategy: &supervision.OneForOne{},
		}),
		af.DefaultSpawnOptions(),
	)
	time.Sleep(50 * time.Millisecond)

	supervision.Register(supRef, "temp", func() af.Actor {
		return newCountActor("temp", nil)
	}, af.DefaultSpawnOptions())
	time.Sleep(50 * time.Millisecond)

	supervision.Unregister(supRef, "temp")
	time.Sleep(50 * time.Millisecond)
}

func TestSupervisor_UnknownActorFailure(t *testing.T) {
	// Sending a failure for an unregistered actor should not panic.
	sys := af.NewActorSystem("test-unknown")
	defer sys.Shutdown()

	supRef, _ := sys.Spawn(
		supervision.NewSupervisorActor("sup3", supervision.SupervisionConfig{
			Strategy: &supervision.OneForOne{},
		}),
		af.DefaultSpawnOptions(),
	)
	time.Sleep(50 * time.Millisecond)

	// Send a mock failure for an unknown actor.
	supRef.Tell(af.Message{
		MsgType: af.MsgActorFailed,
		Payload: af.ActorFailedPayload{ActorID: "ghost", Error: "test"},
	})
	time.Sleep(100 * time.Millisecond)
	// Should not panic.
}

func TestSupervisor_ExponentialBackoff(t *testing.T) {
	sys := af.NewActorSystem("test-backoff")
	defer sys.Shutdown()

	supRef, err := sys.Spawn(
		supervision.NewSupervisorActor("sup4", supervision.SupervisionConfig{
			Strategy: &supervision.ExponentialBackoff{
				InitialDelay: 50 * time.Millisecond,
				MaxDelay:     500 * time.Millisecond,
				Factor:       2.0,
			},
		}),
		af.DefaultSpawnOptions(),
	)
	require.NoError(t, err)

	supervision.Register(supRef, "backoff-actor", func() af.Actor {
		return newCrashActor("backoff-actor")
	}, af.SpawnOptions{SupervisorID: "sup4"})
	time.Sleep(50 * time.Millisecond)

	ref, _ := sys.Spawn(newCrashActor("backoff-actor"), af.SpawnOptions{SupervisorID: "sup4"})
	time.Sleep(50 * time.Millisecond)

	ref.Tell(af.Message{MsgType: "crash"})
	time.Sleep(300 * time.Millisecond)

	_, err = sys.Lookup("backoff-actor")
	require.NoError(t, err)
}

func TestSupervisor_AskAfterRestart(t *testing.T) {
	sys := af.NewActorSystem("test-ask-restart")
	defer sys.Shutdown()

	supRef, _ := sys.Spawn(
		supervision.NewSupervisorActor("sup5", supervision.SupervisionConfig{
			Strategy: &supervision.OneForOne{MaxRetries: 3, Within: 10 * time.Second},
		}),
		af.DefaultSpawnOptions(),
	)

	supervision.Register(supRef, "echo", func() af.Actor {
		return newEchoActor("echo")
	}, af.SpawnOptions{SupervisorID: "sup5"})
	time.Sleep(50 * time.Millisecond)

	ref, _ := sys.Spawn(newEchoActor("echo"), af.SpawnOptions{SupervisorID: "sup5"})
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := ref.Ask(ctx, af.Message{MsgType: "ping", Payload: "hello"})
	require.NoError(t, err)
	require.Equal(t, "hello", reply.Payload)
}

// echoActor used by Ask test.
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
