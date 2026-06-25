// Package actorframework provides a generic, transport-agnostic actor model
// implementation built on Go goroutines and channels.
package actorframework

import (
	"context"
	"fmt"
	"time"
)

// ─────────────────────────────────────────────
// Core types
// ─────────────────────────────────────────────

// ActorID is a unique identifier for every actor in the system.
type ActorID string

// MessageType categorises a message so actors can switch on kind.
type MessageType string

const (
	// System lifecycle messages – sent by ActorSystem, never by user code.
	MsgStarted    MessageType = "system.started"
	MsgStopping   MessageType = "system.stopping"
	MsgStopped    MessageType = "system.stopped"
	MsgRestarting MessageType = "system.restarting"

	// Supervision messages
	MsgActorFailed  MessageType = "supervision.actor_failed"
	MsgRestartActor MessageType = "supervision.restart_actor"
	MsgStatusCheck  MessageType = "supervision.status_check"
	MsgHeartbeat    MessageType = "supervision.heartbeat"
)

// Message is the envelope passed between actors.
// Payload can be any concrete struct; actors type-assert on MsgType.
type Message struct {
	MsgType   MessageType
	Payload   any
	Sender    ActorID   // optional – reply address
	Timestamp time.Time
}

// newMessage is a convenience constructor used internally.
func newMessage(t MessageType, payload any, sender ActorID) Message {
	return Message{
		MsgType:   t,
		Payload:   payload,
		Sender:    sender,
		Timestamp: time.Now(),
	}
}

// ─────────────────────────────────────────────
// Actor interface
// ─────────────────────────────────────────────

// Actor is the contract every actor must satisfy.
//
//   - OnPreStart  – called once before the actor enters its receive loop.
//   - Receive     – called for every message dequeued from the mailbox.
//   - OnPostStop  – called once after the actor has exited its receive loop.
//   - OnPreRestart – called by the supervisor just before it restarts the actor;
//     the error that caused the restart is passed in.
type Actor interface {
	// Lifecycle hooks
	OnPreStart(ctx ActorContext) error
	OnPostStop(ctx ActorContext)
	OnPreRestart(ctx ActorContext, reason error)

	// Message handler – the body of the actor.
	Receive(ctx ActorContext, msg Message)

	// ID returns the actor's unique identifier.
	ID() ActorID
}

// ─────────────────────────────────────────────
// ActorRef – the only handle callers hold
// ─────────────────────────────────────────────

// ActorRef is an opaque reference to a (possibly remote) actor.
// Callers must never access actor state directly; they communicate
// exclusively through ActorRef.
type ActorRef interface {
	// Tell sends a message fire-and-forget (non-blocking).
	Tell(msg Message)

	// Ask sends a message and waits for a single reply, or returns
	// an error if the context deadline is exceeded first.
	Ask(ctx context.Context, msg Message) (Message, error)

	// ID returns the actor's unique identifier.
	ID() ActorID

	// Stop signals the actor to shut down gracefully.
	Stop()
}

// ─────────────────────────────────────────────
// localActorRef – ActorRef backed by a Mailbox
// ─────────────────────────────────────────────

// localActorRef wraps a Mailbox and provides the ActorRef interface
// for actors running inside the same ActorSystem.
type localActorRef struct {
	id      ActorID
	mailbox *Mailbox
	stopCh  chan struct{}
}

func newLocalActorRef(id ActorID, mb *Mailbox, stopCh chan struct{}) *localActorRef {
	return &localActorRef{id: id, mailbox: mb, stopCh: stopCh}
}

func (r *localActorRef) ID() ActorID { return r.id }

// Tell enqueues a message. If the mailbox is full it drops the message
// and prints a warning (bounded-mailbox semantics).
func (r *localActorRef) Tell(msg Message) {
	r.mailbox.Enqueue(msg)
}

// Ask sends msg and blocks until the actor replies on a temporary channel
// embedded in the Payload (ReplyTo field convention), or until ctx expires.
//
// Convention: the responding actor must call ctx.Sender().Tell(reply).
// Ask wraps the message in an AskEnvelope so the actor knows where to reply.
func (r *localActorRef) Ask(ctx context.Context, msg Message) (Message, error) {
	replyCh := make(chan Message, 1)
	envelope := AskEnvelope{Original: msg, ReplyCh: replyCh}
	wrapped := Message{
		MsgType:   msg.MsgType,
		Payload:   envelope,
		Sender:    msg.Sender,
		Timestamp: time.Now(),
	}
	r.mailbox.Enqueue(wrapped)

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		return Message{}, fmt.Errorf("ask timeout waiting for actor %s: %w", r.id, ctx.Err())
	}
}

// Stop sends the Stopping lifecycle message and closes the stop channel.
func (r *localActorRef) Stop() {
	r.mailbox.Enqueue(newMessage(MsgStopping, nil, ""))
}

// ─────────────────────────────────────────────
// AskEnvelope
// ─────────────────────────────────────────────

// AskEnvelope wraps a message sent via Ask so the receiving actor
// can detect it and send a reply back on ReplyCh.
type AskEnvelope struct {
	Original Message
	ReplyCh  chan<- Message
}

// IsAsk returns true when the message payload is an AskEnvelope.
func IsAsk(msg Message) (AskEnvelope, bool) {
	env, ok := msg.Payload.(AskEnvelope)
	return env, ok
}

// ─────────────────────────────────────────────
// BaseActor – optional embedding for common boilerplate
// ─────────────────────────────────────────────

// BaseActor provides no-op default implementations for lifecycle hooks.
// Concrete actors embed BaseActor and override only what they need.
type BaseActor struct {
	id ActorID
}

func NewBaseActor(id ActorID) BaseActor {
	return BaseActor{id: id}
}

func (b *BaseActor) ID() ActorID                                    { return b.id }
func (b *BaseActor) OnPreStart(_ ActorContext) error                { return nil }
func (b *BaseActor) OnPostStop(_ ActorContext)                      {}
func (b *BaseActor) OnPreRestart(_ ActorContext, _ error)           {}
func (b *BaseActor) Receive(_ ActorContext, _ Message)              {}
