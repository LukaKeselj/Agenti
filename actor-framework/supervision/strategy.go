package supervision

import (
	"math"
	"sync"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
)

// RestartAction is the action a RestartStrategy decides on.
type RestartAction int

const (
	ActionRestart  RestartAction = iota // restart the failed actor
	ActionEscalate                      // escalate to parent supervisor
	ActionStop                          // stop – give up on the actor
)

// RestartDecision is the result of consulting a RestartStrategy.
type RestartDecision struct {
	Action RestartAction
	Delay  time.Duration
}

// RestartStrategy determines how a supervisor reacts to actor failures.
type RestartStrategy interface {
	// OnFailure is called when an actor fails.
	// failCount is the number of consecutive failures of this actor.
	// The strategy returns what action to take and an optional delay.
	OnFailure(id af.ActorID, failures []time.Time) RestartDecision

	// Name returns a human-readable strategy name.
	Name() string
}

// ─────────────────────────────────────────────
// OneForOne
// ─────────────────────────────────────────────

// OneForOne restarts only the failed actor. If the actor fails more than
// MaxRetries times within Within, it escalates.
type OneForOne struct {
	MaxRetries int
	Within     time.Duration
}

func (s *OneForOne) Name() string { return "OneForOne" }

func (s *OneForOne) OnFailure(_ af.ActorID, failures []time.Time) RestartDecision {
	if s.MaxRetries <= 0 {
		return RestartDecision{Action: ActionRestart}
	}
	within := s.Within
	if within <= 0 {
		within = 10 * time.Second
	}
	now := time.Now()
	cutoff := now.Add(-within)
	count := 0
	for _, f := range failures {
		if f.After(cutoff) {
			count++
		}
	}
	if count > s.MaxRetries {
		return RestartDecision{Action: ActionEscalate}
	}
	return RestartDecision{Action: ActionRestart}
}

// ─────────────────────────────────────────────
// ExponentialBackoff
// ─────────────────────────────────────────────

// ExponentialBackoff progressively delays restarts.
// Delay = InitialDelay * Factor^(failCount-1), capped at MaxDelay.
type ExponentialBackoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Factor       float64
}

func (s *ExponentialBackoff) Name() string { return "ExponentialBackoff" }

func (s *ExponentialBackoff) OnFailure(_ af.ActorID, failures []time.Time) RestartDecision {
	initial := s.InitialDelay
	if initial <= 0 {
		initial = time.Second
	}
	maxD := s.MaxDelay
	if maxD <= 0 {
		maxD = 30 * time.Second
	}
	factor := s.Factor
	if factor <= 0 {
		factor = 2.0
	}
	n := float64(len(failures))
	delay := time.Duration(float64(initial) * math.Pow(factor, n-1))
	if delay > maxD {
		delay = maxD
	}
	return RestartDecision{Action: ActionRestart, Delay: delay}
}

// ─────────────────────────────────────────────
// Escalation
// ─────────────────────────────────────────────

// Escalation always escalates the failure to the parent supervisor.
// This is the default "let it crash" pattern for leaf actors.
type Escalation struct {
	MaxRetries int
	Within     time.Duration
}

func (s *Escalation) Name() string { return "Escalation" }

func (s *Escalation) OnFailure(_ af.ActorID, failures []time.Time) RestartDecision {
	if s.MaxRetries <= 0 {
		return RestartDecision{Action: ActionEscalate}
	}
	within := s.Within
	if within <= 0 {
		within = 10 * time.Second
	}
	now := time.Now()
	cutoff := now.Add(-within)
	count := 0
	for _, f := range failures {
		if f.After(cutoff) {
			count++
		}
	}
	if count > s.MaxRetries {
		return RestartDecision{Action: ActionStop}
	}
	return RestartDecision{Action: ActionEscalate}
}

// ─────────────────────────────────────────────
// Tracker – thread-safe failure tracking
// ─────────────────────────────────────────────

// FailureTracker keeps timestamps of failures per actor so strategies
// can make window-based decisions.
type FailureTracker struct {
	mu       sync.Mutex
	failures map[af.ActorID][]time.Time
}

func NewFailureTracker() *FailureTracker {
	return &FailureTracker{failures: make(map[af.ActorID][]time.Time)}
}

// Record adds a failure timestamp and returns the current list.
func (t *FailureTracker) Record(id af.ActorID, maxAge time.Duration) []time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	t.failures[id] = append(t.failures[id], now)

	// Prune old entries.
	cutoff := now.Add(-maxAge)
	recent := t.failures[id][:0]
	for _, f := range t.failures[id] {
		if f.After(cutoff) {
			recent = append(recent, f)
		}
	}
	t.failures[id] = recent
	return recent
}

// Reset clears all recorded failures for an actor.
func (t *FailureTracker) Reset(id af.ActorID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.failures, id)
}
