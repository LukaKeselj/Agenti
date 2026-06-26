package supervision

import (
	"context"
	"fmt"
	"sync"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
)

// Message types used by the supervision system.
const (
	MsgRegisterActor   af.MessageType = "supervision.register"
	MsgUnregisterActor af.MessageType = "supervision.unregister"
	MsgDelayedRestart  af.MessageType = "supervision.delayed_restart"
	MsgHeartbeatTick   af.MessageType = "supervision.heartbeat_tick"
)

// RegisterPayload is sent to register an actor for supervision.
type RegisterPayload struct {
	ID      af.ActorID
	Factory func() af.Actor
	Opts    af.SpawnOptions
}

// UnregisterPayload is sent to stop supervising an actor.
type UnregisterPayload struct {
	ID af.ActorID
}

// DelayedRestartPayload carries restart info for delayed execution.
type DelayedRestartPayload struct {
	ID      af.ActorID
	Factory func() af.Actor
	Opts    af.SpawnOptions
}

// SupervisionConfig configures the SupervisorActor.
type SupervisionConfig struct {
	Strategy          RestartStrategy
	HeartbeatInterval time.Duration // 0 disables heartbeat checks
	LoggerID          af.ActorID    // if set, supervisor sends MsgLogEvent here on restart/failure
}

// SupervisorActor monitors actors and applies restart strategies on failure.
type SupervisorActor struct {
	af.BaseActor
	config    SupervisionConfig
	tracker   *FailureTracker
	factories map[af.ActorID]factoryEntry
	mu        sync.RWMutex
	system    *af.ActorSystem
}

type factoryEntry struct {
	factory func() af.Actor
	opts    af.SpawnOptions
}

// NewSupervisorActor creates a new supervisor.
func NewSupervisorActor(id af.ActorID, config SupervisionConfig) *SupervisorActor {
	if config.Strategy == nil {
		config.Strategy = &OneForOne{MaxRetries: 3, Within: 10 * time.Second}
	}
	return &SupervisorActor{
		BaseActor: af.NewBaseActor(id),
		config:    config,
		tracker:   NewFailureTracker(),
		factories: make(map[af.ActorID]factoryEntry),
	}
}

func (s *SupervisorActor) OnPreStart(ctx af.ActorContext) error {
	s.mu.Lock()
	s.system = ctx.System()
	s.mu.Unlock()

	if s.config.HeartbeatInterval > 0 {
		go s.heartbeatLoop(ctx.Self(), ctx.GoContext())
	}
	return nil
}

func (s *SupervisorActor) heartbeatLoop(self af.ActorRef, goCtx context.Context) {
	ticker := time.NewTicker(s.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			self.Tell(af.Message{MsgType: MsgHeartbeatTick})
		case <-goCtx.Done():
			return
		}
	}
}

func (s *SupervisorActor) Receive(ctx af.ActorContext, msg af.Message) {
	if s.system == nil {
		s.mu.Lock()
		s.system = ctx.System()
		s.mu.Unlock()
	}

	switch msg.MsgType {
	case MsgRegisterActor:
		s.handleRegister(msg)
	case MsgUnregisterActor:
		s.handleUnregister(msg)
	case af.MsgActorFailed:
		s.handleFailure(ctx, msg)
	case MsgDelayedRestart:
		s.handleDelayedRestart(ctx, msg)
	case MsgHeartbeatTick:
		s.handleHeartbeatTick(ctx)
	case af.MsgHeartbeat:
		s.handleHeartbeat(msg)
	}
}

// ── handlers ───────────────────────────────────────────────────

func (s *SupervisorActor) handleRegister(msg af.Message) {
	p, ok := msg.Payload.(RegisterPayload)
	if !ok {
		return
	}
	s.mu.Lock()
	s.factories[p.ID] = factoryEntry{factory: p.Factory, opts: p.Opts}
	s.mu.Unlock()
	s.system.Log().Info("actor registered for supervision", "actor_id", p.ID)
}

func (s *SupervisorActor) handleUnregister(msg af.Message) {
	p, ok := msg.Payload.(UnregisterPayload)
	if !ok {
		return
	}
	s.mu.Lock()
	delete(s.factories, p.ID)
	s.tracker.Reset(p.ID)
	s.mu.Unlock()
	s.system.Log().Info("actor unregistered from supervision", "actor_id", p.ID)
}

func (s *SupervisorActor) handleFailure(ctx af.ActorContext, msg af.Message) {
	payload, ok := msg.Payload.(af.ActorFailedPayload)
	if !ok {
		return
	}

	s.mu.RLock()
	entry, hasFactory := s.factories[payload.ActorID]
	s.mu.RUnlock()

	if !hasFactory {
		s.system.Log().Warn("received failure for unregistered actor",
			"actor_id", payload.ActorID)
		return
	}

	failures := s.tracker.Record(payload.ActorID, 30*time.Second)
	payload.RestartCount = len(failures)
	decision := s.config.Strategy.OnFailure(payload.ActorID, failures)

	switch decision.Action {
	case ActionRestart:
		if decision.Delay > 0 {
			s.scheduleDelayedRestart(ctx, payload.ActorID, entry, decision.Delay)
		} else {
			s.doRestart(ctx, payload.ActorID, entry)
		}

	case ActionEscalate:
		s.doEscalate(payload)

	case ActionStop:
		s.system.Log().Info("stopped supervising actor (strategy decision)",
			"actor_id", payload.ActorID)
		s.mu.Lock()
		delete(s.factories, payload.ActorID)
		s.mu.Unlock()
	}
}

func (s *SupervisorActor) doRestart(_ af.ActorContext, id af.ActorID, entry factoryEntry) {
	newRef, err := s.system.RestartActor(entry.factory, entry.opts)
	if err != nil {
		s.system.Log().Error("failed to restart actor",
			"actor_id", id, "err", err)
		return
	}
	s.mu.Lock()
	s.factories[id] = entry
	s.tracker.Reset(id)
	s.mu.Unlock()
	s.system.Log().Info("actor restarted", "actor_id", id, "new_ref", newRef.ID())

	// Spec §4.2.1: log restart event to LoggerActor.
	// Payload matches LogEventPayload{Source, Event} via JSON round-trip in castPayload.
	if s.config.LoggerID != "" {
		if logRef, lookupErr := s.system.Lookup(s.config.LoggerID); lookupErr == nil {
			logRef.Tell(af.Message{
				MsgType: "smart_home.log_event",
				Payload: map[string]any{
					"Source": fmt.Sprintf("supervisor/%s", s.ID()),
					"Event":  fmt.Sprintf("actor %q restarted by supervisor", id),
				},
			})
		}
	}
}

func (s *SupervisorActor) doEscalate(payload af.ActorFailedPayload) {
	s.mu.RLock()
	entry, hasFactory := s.factories[payload.ActorID]
	s.mu.RUnlock()
	if !hasFactory {
		return
	}
	supervisorID := entry.opts.SupervisorID
	if supervisorID == "" {
		s.system.Log().Warn("no parent supervisor to escalate to",
			"actor_id", payload.ActorID)
		return
	}
	parentRef, err := s.system.Lookup(supervisorID)
	if err != nil {
		s.system.Log().Warn("parent supervisor not found",
			"supervisor_id", supervisorID)
		return
	}
	parentRef.Tell(af.Message{
		MsgType: af.MsgActorFailed,
		Payload: payload,
	})
}

func (s *SupervisorActor) scheduleDelayedRestart(ctx af.ActorContext, id af.ActorID, entry factoryEntry, delay time.Duration) {
	go func() {
		select {
		case <-time.After(delay):
			ctx.Self().Tell(af.Message{
				MsgType: MsgDelayedRestart,
				Payload: DelayedRestartPayload{
					ID:      id,
					Factory: entry.factory,
					Opts:    entry.opts,
				},
			})
		case <-ctx.GoContext().Done():
		}
	}()
}

func (s *SupervisorActor) handleDelayedRestart(ctx af.ActorContext, msg af.Message) {
	p, ok := msg.Payload.(DelayedRestartPayload)
	if !ok {
		return
	}
	s.doRestart(ctx, p.ID, factoryEntry{factory: p.Factory, opts: p.Opts})
}

// ── heartbeat ──────────────────────────────────────────────────

func (s *SupervisorActor) handleHeartbeatTick(ctx af.ActorContext) {
	s.mu.RLock()
	ids := make([]af.ActorID, 0, len(s.factories))
	for id := range s.factories {
		ids = append(ids, id)
	}
	s.mu.RUnlock()

	for _, id := range ids {
		ref, err := s.system.Lookup(id)
		if err != nil {
			continue
		}
		ref.Tell(af.Message{
			MsgType: af.MsgStatusCheck,
			Sender:  ctx.Self().ID(),
		})
	}
}

func (s *SupervisorActor) handleHeartbeat(msg af.Message) {
	s.system.Log().Debug("heartbeat received", "from", msg.Sender)
}

// ── helper ─────────────────────────────────────────────────────

// Register registers an actor with the supervisor by sending a
// RegisterActor message to the supervisor's ActorRef.
func Register(ref af.ActorRef, id af.ActorID, factory func() af.Actor, opts af.SpawnOptions) {
	ref.Tell(af.Message{
		MsgType: MsgRegisterActor,
		Payload: RegisterPayload{ID: id, Factory: factory, Opts: opts},
	})
}

// Unregister removes an actor from supervision.
func Unregister(ref af.ActorRef, id af.ActorID) {
	ref.Tell(af.Message{
		MsgType: MsgUnregisterActor,
		Payload: UnregisterPayload{ID: id},
	})
}
