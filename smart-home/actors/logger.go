package actors

import (
	"sync"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/smart-home/persistence"
)

// ── LoggerActor ────────────────────────────────────────────────

// LoggerActor collects metrics, round completions, events, and
// device status messages from all actors and stores them in memory.
type LoggerActor struct {
	af.BaseActor

	mu        sync.Mutex
	metrics   []LogMetricsPayload
	rounds    []RoundCompletePayload
	events    []LogEventPayload
	devices   []DeviceStatusPayload
	persister *persistence.Persister
}

type loggerSavedState struct {
	Rounds  []RoundCompletePayload `json:"rounds"`
	Metrics []LogMetricsPayload    `json:"metrics"`
	Events  []LogEventPayload      `json:"events"`
	Devices []DeviceStatusPayload  `json:"devices"`
}

func (l *LoggerActor) SetPersister(p *persistence.Persister) {
	l.persister = p
}

func (l *LoggerActor) OnPreStart(ctx af.ActorContext) error {
	if l.persister == nil {
		return nil
	}
	var state loggerSavedState
	if err := l.persister.Load(string(l.ID()), &state); err != nil {
		return nil
	}
	l.mu.Lock()
	l.rounds = state.Rounds
	l.metrics = state.Metrics
	l.events = state.Events
	l.devices = state.Devices
	if l.rounds == nil {
		l.rounds = make([]RoundCompletePayload, 0)
	}
	if l.metrics == nil {
		l.metrics = make([]LogMetricsPayload, 0)
	}
	if l.events == nil {
		l.events = make([]LogEventPayload, 0)
	}
	if l.devices == nil {
		l.devices = make([]DeviceStatusPayload, 0)
	}
	l.mu.Unlock()
	ctx.Log().Info("restored logger state", "rounds", len(state.Rounds))
	return nil
}

func (l *LoggerActor) persistState() {
	if l.persister == nil {
		return
	}
	l.mu.Lock()
	state := loggerSavedState{
		Rounds:  l.rounds,
		Metrics: l.metrics,
		Events:  l.events,
		Devices: l.devices,
	}
	l.mu.Unlock()
	_ = l.persister.Save(string(l.ID()), state)
}

// NewLoggerActor creates a new logger.
func NewLoggerActor(id af.ActorID) *LoggerActor {
	return &LoggerActor{
		BaseActor: af.NewBaseActor(id),
	}
}

func (l *LoggerActor) Receive(_ af.ActorContext, msg af.Message) {
	switch msg.MsgType {
	case MsgRoundComplete:
		p, ok := msg.Payload.(RoundCompletePayload)
		if !ok {
			return
		}
		l.mu.Lock()
		l.rounds = append(l.rounds, p)
		l.mu.Unlock()
		l.persistState()

	case MsgLogMetrics:
		p, ok := msg.Payload.(LogMetricsPayload)
		if !ok {
			return
		}
		l.mu.Lock()
		l.metrics = append(l.metrics, p)
		l.mu.Unlock()
		l.persistState()

	case MsgLogEvent:
		p, ok := msg.Payload.(LogEventPayload)
		if !ok {
			return
		}
		l.mu.Lock()
		l.events = append(l.events, p)
		l.mu.Unlock()
		l.persistState()

	case MsgDeviceStatus:
		p, ok := msg.Payload.(DeviceStatusPayload)
		if !ok {
			return
		}
		l.mu.Lock()
		l.devices = append(l.devices, p)
		l.mu.Unlock()
		l.persistState()
	}
}

// Rounds returns all recorded round completions.
func (l *LoggerActor) Rounds() []RoundCompletePayload {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]RoundCompletePayload, len(l.rounds))
	copy(out, l.rounds)
	return out
}

// Metrics returns all recorded metric entries.
func (l *LoggerActor) Metrics() []LogMetricsPayload {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LogMetricsPayload, len(l.metrics))
	copy(out, l.metrics)
	return out
}

// Events returns all recorded events.
func (l *LoggerActor) Events() []LogEventPayload {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LogEventPayload, len(l.events))
	copy(out, l.events)
	return out
}

// DeviceStatuses returns all recorded device status reports.
func (l *LoggerActor) DeviceStatuses() []DeviceStatusPayload {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]DeviceStatusPayload, len(l.devices))
	copy(out, l.devices)
	return out
}

// LastRound returns the most recent round completion, if any.
func (l *LoggerActor) LastRound() (RoundCompletePayload, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.rounds) == 0 {
		return RoundCompletePayload{}, false
	}
	return l.rounds[len(l.rounds)-1], true
}
