package actorframework

import (
	"context"
	"log/slog"
)

// ─────────────────────────────────────────────
// BehaviorFunc – the heart of Become/Unbecome
// ─────────────────────────────────────────────

// BehaviorFunc is a message-handling function that can be hot-swapped
// at runtime via ActorContext.Become / ActorContext.Unbecome.
//
// When an actor calls ctx.Become(newBehavior), the next message dequeued
// from the mailbox will be dispatched to newBehavior instead of the
// actor's default Receive method.  This models finite-state machines
// naturally: each state is a BehaviorFunc.
//
// Example state machine for SensorActor:
//
//	Idle ──StartTraining──▶ Training ──ModelSent──▶ SendingResults ──Done──▶ Idle
type BehaviorFunc func(ctx ActorContext, msg Message)

// ─────────────────────────────────────────────
// ActorContext interface
// ─────────────────────────────────────────────

// ActorContext is the capability object passed into every Receive call and
// into lifecycle hooks.  It gives an actor access to:
//   - Its own identity and self-reference
//   - The ActorSystem (to spawn children, look up peers)
//   - Behaviour switching (Become / Unbecome)
//   - Logging
//   - The parent Go context (for cancellation / deadlines)
type ActorContext interface {
	// Self returns an ActorRef pointing back to the current actor.
	// Use this as the Sender field when sending messages, so the
	// recipient knows where to reply.
	Self() ActorRef

	// System gives access to the ActorSystem for spawning and lookup.
	System() *ActorSystem

	// Sender returns an ActorRef of the actor that sent the current message,
	// if the Sender field was populated.  Returns nil when not set.
	Sender() ActorRef

	// Become replaces the current message handler with newBehavior.
	// The stack of previous behaviors is retained for Unbecome.
	Become(newBehavior BehaviorFunc)

	// Unbecome pops the most recent Become, reverting to the previous
	// behavior.  If the stack is empty it is a no-op.
	Unbecome()

	// CurrentBehavior returns the active BehaviorFunc, or nil if the
	// actor's own Receive method is active.
	CurrentBehavior() BehaviorFunc

	// Log returns a structured logger tagged with the actor's ID.
	Log() *slog.Logger

	// GoContext returns the Go context tied to this actor's lifetime.
	// It is cancelled when the actor is stopped.
	GoContext() context.Context
}

// ─────────────────────────────────────────────
// actorContext – concrete implementation
// ─────────────────────────────────────────────

type actorContext struct {
	self    *localActorRef
	system  *ActorSystem
	sender  ActorRef       // populated per-message by the run loop
	logger  *slog.Logger
	goCtx   context.Context
	cancel  context.CancelFunc

	// Behavior stack – index 0 is the base (actor.Receive).
	// Become pushes onto behaviorStack; Unbecome pops.
	behaviorStack []BehaviorFunc
}

func newActorContext(
	self *localActorRef,
	system *ActorSystem,
	goCtx context.Context,
	cancel context.CancelFunc,
) *actorContext {
	return &actorContext{
		self:   self,
		system: system,
		logger: slog.Default().With("actor_id", string(self.id)),
		goCtx:  goCtx,
		cancel: cancel,
	}
}

func (c *actorContext) Self() ActorRef    { return c.self }
func (c *actorContext) System() *ActorSystem { return c.system }
func (c *actorContext) Log() *slog.Logger { return c.logger }
func (c *actorContext) GoContext() context.Context { return c.goCtx }

func (c *actorContext) Sender() ActorRef {
	return c.sender
}

// setSender is called by the run loop before each Receive dispatch.
func (c *actorContext) setSender(ref ActorRef) {
	c.sender = ref
}

// ─── Behavior switching ────────────────────────────────────────────────────

func (c *actorContext) Become(newBehavior BehaviorFunc) {
	c.behaviorStack = append(c.behaviorStack, newBehavior)
	c.logger.Debug("behavior changed", "depth", len(c.behaviorStack))
}

func (c *actorContext) Unbecome() {
	if len(c.behaviorStack) == 0 {
		return
	}
	c.behaviorStack = c.behaviorStack[:len(c.behaviorStack)-1]
	c.logger.Debug("behavior reverted", "depth", len(c.behaviorStack))
}

func (c *actorContext) CurrentBehavior() BehaviorFunc {
	if len(c.behaviorStack) == 0 {
		return nil
	}
	return c.behaviorStack[len(c.behaviorStack)-1]
}

// dispatch is called by the run loop to deliver a message.
// It uses the top of the behavior stack if present,
// otherwise it delegates to the actor's own Receive.
func (c *actorContext) dispatch(actor Actor, msg Message) {
	// Populate sender from message envelope.
	if msg.Sender != "" {
		if ref, err := c.system.Lookup(msg.Sender); err == nil {
			c.setSender(ref)
		} else {
			c.setSender(nil)
		}
	} else {
		c.setSender(nil)
	}

	// System lifecycle messages always go to the actor's own Receive,
	// never to a hot-swapped behavior.  This prevents behavior code from
	// accidentally intercepting Started / Stopped signals.
	isLifecycle := msg.MsgType == MsgStarted ||
		msg.MsgType == MsgStopping ||
		msg.MsgType == MsgStopped ||
		msg.MsgType == MsgRestarting

	if !isLifecycle {
		if behavior := c.CurrentBehavior(); behavior != nil {
			behavior(c, msg)
			return
		}
	}
	actor.Receive(c, msg)
}
