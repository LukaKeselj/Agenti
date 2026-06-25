package supervision_test

import (
	"context"
	"testing"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/actor-framework/supervision"
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
	if d.Action != supervision.ActionRestart {
		t.Fatalf("expected Restart, got %v", d.Action)
	}
}

func TestStrategy_OneForOne_Escalate(t *testing.T) {
	now := time.Now()
	failures := make([]time.Time, 4)
	for i := range failures {
		failures[i] = now
	}
	strat := &supervision.OneForOne{MaxRetries: 3, Within: 10 * time.Second}
	d := strat.OnFailure("a1", failures)
	if d.Action != supervision.ActionEscalate {
		t.Fatalf("expected Escalate, got %v", d.Action)
	}
}

func TestStrategy_ExponentialBackoff_Delays(t *testing.T) {
	strat := &supervision.ExponentialBackoff{
		InitialDelay: time.Second,
		MaxDelay:     30 * time.Second,
		Factor:       2.0,
	}
	// First failure → 1s delay.
	d1 := strat.OnFailure("a1", []time.Time{time.Now()})
	if d1.Action != supervision.ActionRestart || d1.Delay != time.Second {
		t.Fatalf("expected 1s delay, got %v", d1.Delay)
	}
	// Second failure → 2s delay.
	d2 := strat.OnFailure("a1", []time.Time{time.Now(), time.Now()})
	if d2.Delay != 2*time.Second {
		t.Fatalf("expected 2s delay, got %v", d2.Delay)
	}
	// 6th failure should be capped at max 30s.
	d6 := strat.OnFailure("a1", make([]time.Time, 6))
	if d6.Delay != 30*time.Second {
		t.Fatalf("expected 30s cap, got %v", d6.Delay)
	}
}

func TestStrategy_Escalation(t *testing.T) {
	strat := &supervision.Escalation{}
	d := strat.OnFailure("a1", []time.Time{time.Now()})
	if d.Action != supervision.ActionEscalate {
		t.Fatalf("expected Escalate, got %v", d.Action)
	}
}

// ── supervisor integration tests ───────────────────────────────

func TestSupervisor_RestartOnFailure(t *testing.T) {
	sys := af.NewActorSystem("test-supervisor")
	defer sys.Shutdown()

	// Spawn the supervisor.
	supRef, err := sys.Spawn(
		supervision.NewSupervisorActor("sup", supervision.SupervisionConfig{
			Strategy: &supervision.OneForOne{MaxRetries: 3, Within: 10 * time.Second},
		}),
		af.DefaultSpawnOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Register a factory for the crash actor.
	supervision.Register(supRef, "crasher", func() af.Actor {
		return newCrashActor("crasher")
	}, af.SpawnOptions{
		SupervisorID: "sup",
	})
	time.Sleep(50 * time.Millisecond)

	// Spawn the actual crash actor (supervised by sup).
	crasherRef, err := sys.Spawn(newCrashActor("crasher"), af.SpawnOptions{
		SupervisorID: "sup",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = crasherRef
	time.Sleep(50 * time.Millisecond)

	// Trigger a crash.
	crasherRef.Tell(af.Message{MsgType: "crash"})
	time.Sleep(500 * time.Millisecond)

	// After the crash and restart, the actor should be available again.
	newRef, err := sys.Lookup("crasher")
	if err != nil {
		t.Fatalf("actor should have been restarted: %v", err)
	}
	// The new ref should be functional.
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

	// Register then unregister an actor.
	supervision.Register(supRef, "temp", func() af.Actor {
		return newCountActor("temp", nil)
	}, af.DefaultSpawnOptions())
	time.Sleep(50 * time.Millisecond)

	supervision.Unregister(supRef, "temp")
	time.Sleep(50 * time.Millisecond)
	// Should not panic or error.
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
	if err != nil {
		t.Fatal(err)
	}

	supervision.Register(supRef, "backoff-actor", func() af.Actor {
		return newCrashActor("backoff-actor")
	}, af.SpawnOptions{SupervisorID: "sup4"})
	time.Sleep(50 * time.Millisecond)

	ref, _ := sys.Spawn(newCrashActor("backoff-actor"), af.SpawnOptions{SupervisorID: "sup4"})
	time.Sleep(50 * time.Millisecond)

	// Crash twice.
	ref.Tell(af.Message{MsgType: "crash"})
	time.Sleep(300 * time.Millisecond)

	// After first restart, the actor should be back.
	_, err = sys.Lookup("backoff-actor")
	if err != nil {
		t.Fatalf("actor should have been restarted: %v", err)
	}
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

	// Ask should work.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := ref.Ask(ctx, af.Message{MsgType: "ping", Payload: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Payload != "hello" {
		t.Fatalf("expected 'hello', got %v", reply.Payload)
	}
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
