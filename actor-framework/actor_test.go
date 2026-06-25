package actorframework_test

import (
	"context"
	"sync"
	"testing"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	mb.Enqueue(af.Message{MsgType: "c"})

	stats := mb.Stats()
	require.EqualValues(t, 1, stats.Dropped)
	require.EqualValues(t, 2, stats.Pending)
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
			require.Failf(t, "timeout", "only received %d/%d messages", received, n)
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
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		ref.Tell(af.Message{MsgType: "ping"})
	}

	select {
	case <-actor.done:
	case <-time.After(2 * time.Second):
		require.Failf(t, "timeout", "got %d messages", actor.Count())
	}

	require.Equal(t, 3, actor.Count())
}

func TestSystem_DuplicateSpawnFails(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	opts := af.DefaultSpawnOptions()
	_, err := sys.Spawn(newEchoActor("echo"), opts)
	require.NoError(t, err)

	_, err = sys.Spawn(newEchoActor("echo"), opts)
	require.Error(t, err)
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
	require.NoError(t, err)
	require.Equal(t, "world", reply.Payload)
}

func TestSystem_AskTimeout(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	ref, _ := sys.Spawn(newCountActor("sink", 999), af.DefaultSpawnOptions())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := ref.Ask(ctx, af.Message{MsgType: "req"})
	require.Error(t, err)
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

	time.Sleep(100 * time.Millisecond)

	actor.mu.Lock()
	got := actor.received
	actor.mu.Unlock()

	want := []string{"default:before", "default:switch", "alt:after"}
	require.Len(t, got, len(want))
	for i, v := range want {
		assert.Equal(t, v, got[i], "index %d", i)
	}
}

// ─────────────────────────────────────────────
// Lookup
// ─────────────────────────────────────────────

func TestSystem_Lookup(t *testing.T) {
	sys := af.NewActorSystem("test")
	defer sys.Shutdown()

	sys.Spawn(newEchoActor("alpha"), af.DefaultSpawnOptions())

	_, err := sys.Lookup("alpha")
	require.NoError(t, err)

	_, err = sys.Lookup("ghost")
	require.Error(t, err)
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

	done := make(chan struct{})
	go func() { sys.Shutdown(); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		require.Fail(t, "Shutdown did not complete in time")
	}
}
