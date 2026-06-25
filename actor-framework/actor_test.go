package actorframework_test

import (
	"context"
	"sync"
	"testing"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
)

// ─────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────

// echoActor replies to every non-system message with the same payload.
type echoActor struct {
	af.BaseActor
}

func newEchoActor(id af.ActorID) *echoActor {
	return &echoActor{BaseActor: af.NewBaseActor(id)}
}

func (a *echoActor) Receive(ctx af.ActorContext, msg af.Message) {
	if env, ok := af.IsAsk(msg); ok {
		env.ReplyCh <- af.Message{
			MsgType: msg.MsgType,
			Payload: env.Original.Payload,
		}
		return
	}
	// fire-and-forget: do nothing
}

// countActor counts how many messages it receives.
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

// becomeActor switches behavior after the first "switch" message.
type becomeActor struct {
	af.BaseActor
	received []string
	mu       sync.Mutex
}

func newBecomeActor(id af.ActorID) *becomeActor {
	return &becomeActor{BaseActor: af.NewBaseActor(id)}
}

func (a *becomeActor) Receive(ctx af.ActorContext, msg af.Message) {
	if msg.MsgType == af.MsgStarted {
		return
	}
	a.mu.Lock()
	a.received = append(a.received, "default:"+string(msg.MsgType))
	a.mu.Unlock()

	if msg.MsgType == "switch" {
		ctx.Become(func(ctx af.ActorContext, msg af.Message) {
			a.mu.Lock()
			a.received = append(a.received, "alt:"+string(msg.MsgType))
			a.mu.Unlock()
		})
	}
}

// ─────────────────────────────────────────────
// Mailbox tests
// ─────────────────────────────────────────────

func TestMailbox_BoundedDropNewest(t *testing.T) {
	mb := af.NewMailbox(2, af.DropNewest)

	mb.Enqueue(af.Message{MsgType: "a"})
	mb.Enqueue(af.Message{MsgType: "b"})
	mb.Enqueue(af.Message{MsgType: "c"}) // should be dropped

	stats := mb.Stats()
	if stats.Dropped != 1 {
		t.Fatalf("expected 1 dropped, got %d", stats.Dropped)
	}
	if stats.Pending != 2 {
		t.Fatalf("expected 2 pending, got %d", stats.Pending)
	}
}

func TestMailbox_Unbounded(t *testing.T) {
	mb := af.NewUnboundedMailbox()
	const n = 1000
	for i := 0; i < n; i++ {
		mb.Enqueue(af.Message{MsgType: "x"})
	}

	received := 0
	timeout := time.After(2 * time.Second)
	for received < n {
		select {
		case <-mb.C():
			received++
		case <-timeout:
			t.Fatalf("timeout: only received %d/%d messages", received, n)
		}
	}
}

// ─────────────────────────────────────────────
// ActorSystem – spawn & Tell
// ─────────────────────────────────────────────

func TestSystem_SpawnAndTell(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	actor := newCountActor("counter", 3)
	ref, err := sys.Spawn(actor, af.DefaultSpawnOptions())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		ref.Tell(af.Message{MsgType: "ping"})
	}

	select {
	case <-actor.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out; got %d messages", actor.Count())
	}

	if actor.Count() != 3 {
		t.Fatalf("expected 3, got %d", actor.Count())
	}
}

func TestSystem_DuplicateSpawnFails(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	opts := af.DefaultSpawnOptions()
	_, err := sys.Spawn(newEchoActor("echo"), opts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sys.Spawn(newEchoActor("echo"), opts)
	if err == nil {
		t.Fatal("expected error on duplicate spawn, got nil")
	}
}

// ─────────────────────────────────────────────
// Ask / reply
// ─────────────────────────────────────────────

func TestSystem_Ask(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	ref, _ := sys.Spawn(newEchoActor("echo"), af.DefaultSpawnOptions())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reply, err := ref.Ask(ctx, af.Message{MsgType: "hello", Payload: "world"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Payload != "world" {
		t.Fatalf("expected 'world', got %v", reply.Payload)
	}
}

func TestSystem_AskTimeout(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	// A counter actor never replies to Ask, so Ask must time out.
	ref, _ := sys.Spawn(newCountActor("sink", 999), af.DefaultSpawnOptions())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := ref.Ask(ctx, af.Message{MsgType: "req"})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

// ─────────────────────────────────────────────
// Become / Unbecome
// ─────────────────────────────────────────────

func TestContext_BecomeUnbecome(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	actor := newBecomeActor("sm")
	ref, _ := sys.Spawn(actor, af.DefaultSpawnOptions())

	ref.Tell(af.Message{MsgType: "before"})
	ref.Tell(af.Message{MsgType: "switch"})
	ref.Tell(af.Message{MsgType: "after"})

	// Give the actor time to process.
	time.Sleep(100 * time.Millisecond)

	actor.mu.Lock()
	got := actor.received
	actor.mu.Unlock()

	want := []string{"default:before", "default:switch", "alt:after"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i, v := range want {
		if got[i] != v {
			t.Errorf("index %d: expected %q, got %q", i, v, got[i])
		}
	}
}

// ─────────────────────────────────────────────
// Lookup
// ─────────────────────────────────────────────

func TestSystem_Lookup(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	sys.Spawn(newEchoActor("alpha"), af.DefaultSpawnOptions())

	if _, err := sys.Lookup("alpha"); err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if _, err := sys.Lookup("ghost"); err == nil {
		t.Fatal("expected error for unknown actor")
	}
}

// ─────────────────────────────────────────────
// Graceful shutdown
// ─────────────────────────────────────────────

func TestSystem_Shutdown(t *testing.T) {
	sys := af.NewActorSystem("test")
	for i := 0; i < 5; i++ {
		id := af.ActorID(string(rune('a' + i)))
		sys.Spawn(newCountActor(id, 0), af.DefaultSpawnOptions())
	}
	// Shutdown must return within a reasonable timeout.
	done := make(chan struct{})
	go func() { sys.Shutdown(); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not complete in time")
	}
}
