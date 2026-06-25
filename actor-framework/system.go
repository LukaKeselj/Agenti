package actorframework

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// ─────────────────────────────────────────────
// SpawnOptions
// ─────────────────────────────────────────────

// SpawnOptions configures how an actor is created.
type SpawnOptions struct {
	// MailboxCapacity sets the bounded mailbox size.
	// 0 means unbounded.
	MailboxCapacity int

	// MailboxStrategy controls overflow behaviour for bounded mailboxes.
	MailboxStrategy MailboxStrategy

	// SupervisorID, when set, registers this actor under a supervisor.
	// The supervisor will be notified on actor failure.
	SupervisorID ActorID
}

// DefaultSpawnOptions returns sensible defaults.
func DefaultSpawnOptions() SpawnOptions {
	return SpawnOptions{
		MailboxCapacity: DefaultMailboxSize,
		MailboxStrategy: DropNewest,
	}
}

// ─────────────────────────────────────────────
// actorEntry – internal bookkeeping per actor
// ─────────────────────────────────────────────

type actorEntry struct {
	actor      Actor
	ref        *localActorRef
	ctx        *actorContext
	cancelFunc context.CancelFunc
	opts       SpawnOptions
}

// ─────────────────────────────────────────────
// ActorSystem
// ─────────────────────────────────────────────

// ActorSystem is the runtime that owns, spawns, and tears down actors.
// A single ActorSystem per process is typical; create one with NewActorSystem.
//
// ActorSystem is safe for concurrent use.
type ActorSystem struct {
	name   string
	mu     sync.RWMutex
	actors map[ActorID]*actorEntry
	wg     sync.WaitGroup
	log    *slog.Logger

	// closed is closed when Shutdown is called.
	closed chan struct{}
	once   sync.Once
}

// NewActorSystem creates and starts a named ActorSystem.
func NewActorSystem(name string) *ActorSystem {
	return &ActorSystem{
		name:   name,
		actors: make(map[ActorID]*actorEntry),
		log:    slog.Default().With("system", name),
		closed: make(chan struct{}),
	}
}

// Name returns the system's name.
func (s *ActorSystem) Name() string { return s.name }

// ─────────────────────────────────────────────
// Spawn
// ─────────────────────────────────────────────

// Spawn registers and starts actor under the given ID.
// Returns an ActorRef the caller can use to send messages.
//
// Spawn is safe to call from multiple goroutines, including from within
// an actor's Receive method.
func (s *ActorSystem) Spawn(actor Actor, opts SpawnOptions) (ActorRef, error) {
	select {
	case <-s.closed:
		return nil, errors.New("actorsystem: cannot spawn into a shut-down system")
	default:
	}

	id := actor.ID()

	s.mu.Lock()
	if _, exists := s.actors[id]; exists {
		s.mu.Unlock()
		return nil, fmt.Errorf("actorsystem: actor %q already registered", id)
	}

	// Build mailbox.
	var mb *Mailbox
	if opts.MailboxCapacity == 0 {
		mb = NewUnboundedMailbox()
	} else {
		mb = NewMailbox(opts.MailboxCapacity, opts.MailboxStrategy)
	}

	stopCh := make(chan struct{})
	ref := newLocalActorRef(id, mb, stopCh)

	goCtx, cancel := context.WithCancel(context.Background())
	actCtx := newActorContext(ref, s, goCtx, cancel)

	entry := &actorEntry{
		actor:      actor,
		ref:        ref,
		ctx:        actCtx,
		cancelFunc: cancel,
		opts:       opts,
	}
	s.actors[id] = entry
	s.mu.Unlock()

	// Start the actor's goroutine.
	s.wg.Add(1)
	go s.runActor(entry)

	s.log.Info("actor spawned", "id", id)
	return ref, nil
}

// MustSpawn is like Spawn but panics on error (useful in tests / init code).
func (s *ActorSystem) MustSpawn(actor Actor, opts SpawnOptions) ActorRef {
	ref, err := s.Spawn(actor, opts)
	if err != nil {
		panic(err)
	}
	return ref
}

// ─────────────────────────────────────────────
// runActor – the goroutine that drives each actor
// ─────────────────────────────────────────────

func (s *ActorSystem) runActor(e *actorEntry) {
	defer s.wg.Done()
	defer func() {
		// Recover from panics so one bad actor cannot crash the system.
		if r := recover(); r != nil {
			err := fmt.Errorf("panic in actor %s: %v", e.actor.ID(), r)
			s.log.Error("actor panicked", "id", e.actor.ID(), "err", err)
			s.handleActorFailure(e, err)
		}
	}()

	// OnPreStart lifecycle hook.
	if err := e.actor.OnPreStart(e.ctx); err != nil {
		s.log.Error("OnPreStart failed", "id", e.actor.ID(), "err", err)
		s.handleActorFailure(e, err)
		return
	}

	// Notify the actor itself that it has started.
	e.ref.mailbox.Enqueue(newMessage(MsgStarted, nil, ""))

	// ── Main receive loop ────────────────────────────────────────────────
	for {
		select {
		case msg, ok := <-e.ref.mailbox.C():
			if !ok {
				// Mailbox closed – treat as graceful stop.
				goto stopped
			}

			switch msg.MsgType {
			case MsgStopping:
				// Drain remaining messages, then stop.
				goto stopped

			default:
				e.ctx.dispatch(e.actor, msg)
			}

		case <-s.closed:
			// System-wide shutdown.
			goto stopped
		}
	}

stopped:
	e.actor.OnPostStop(e.ctx)
	e.cancelFunc()

	s.mu.Lock()
	delete(s.actors, e.actor.ID())
	s.mu.Unlock()

	s.log.Info("actor stopped", "id", e.actor.ID())
}

// ─────────────────────────────────────────────
// Failure handling
// ─────────────────────────────────────────────

// handleActorFailure is called when an actor panics or its OnPreStart fails.
// If the actor was registered under a supervisor, the supervisor is notified.
func (s *ActorSystem) handleActorFailure(e *actorEntry, reason error) {
	// Remove the failed actor from the registry.
	s.mu.Lock()
	delete(s.actors, e.actor.ID())
	s.mu.Unlock()

	e.cancelFunc()

	if e.opts.SupervisorID == "" {
		return
	}

	// Notify supervisor.
	supervisorRef, err := s.Lookup(e.opts.SupervisorID)
	if err != nil {
		s.log.Warn("supervisor not found for failed actor",
			"actor", e.actor.ID(), "supervisor", e.opts.SupervisorID)
		return
	}

	supervisorRef.Tell(newMessage(MsgActorFailed, ActorFailedPayload{
		ActorID:   e.actor.ID(),
		ActorType: fmt.Sprintf("%T", e.actor),
		Error:     reason.Error(),
	}, ""))
}

// ─────────────────────────────────────────────
// Lookup
// ─────────────────────────────────────────────

// Lookup returns an ActorRef for the actor with the given ID, or an error
// if no such actor is currently registered.
func (s *ActorSystem) Lookup(id ActorID) (ActorRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.actors[id]
	if !ok {
		return nil, fmt.Errorf("actorsystem: actor %q not found", id)
	}
	return entry.ref, nil
}

// ─────────────────────────────────────────────
// Stop a single actor
// ─────────────────────────────────────────────

// StopActor gracefully stops a single actor by ID.
func (s *ActorSystem) StopActor(id ActorID) error {
	ref, err := s.Lookup(id)
	if err != nil {
		return err
	}
	ref.Stop()
	return nil
}

// RestartActor stops the existing actor and spawns a fresh instance.
// The actor must implement a zero-argument-like constructor; the caller
// provides a factory function.
func (s *ActorSystem) RestartActor(factory func() Actor, opts SpawnOptions) (ActorRef, error) {
	// The factory creates a new actor with the same ID.
	newActor := factory()
	id := newActor.ID()

	// Stop the old one (ignore error – it may already be gone).
	_ = s.StopActor(id)

	return s.Spawn(newActor, opts)
}

// ─────────────────────────────────────────────
// Shutdown
// ─────────────────────────────────────────────

// Shutdown gracefully stops all actors and waits for them to finish.
// After Shutdown returns, the ActorSystem must not be used.
func (s *ActorSystem) Shutdown() {
	s.once.Do(func() {
		s.log.Info("system shutting down")
		close(s.closed)

		// Send Stopping to all registered actors.
		s.mu.RLock()
		refs := make([]*localActorRef, 0, len(s.actors))
		for _, e := range s.actors {
			refs = append(refs, e.ref)
		}
		s.mu.RUnlock()

		for _, ref := range refs {
			ref.mailbox.Enqueue(newMessage(MsgStopping, nil, ""))
		}

		s.wg.Wait()
		s.log.Info("system stopped")
	})
}

// ─────────────────────────────────────────────
// Payload types used by the system
// ─────────────────────────────────────────────

// ActorFailedPayload is sent to a supervisor when an actor fails.
type ActorFailedPayload struct {
	ActorID   ActorID
	ActorType string
	Error     string
}

// RestartActorPayload is sent by a supervisor to instruct the system
// to restart a specific actor.
type RestartActorPayload struct {
	ActorID  ActorID
	Strategy string
	DelayMs  int64
}
