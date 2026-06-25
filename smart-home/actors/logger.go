package actors

import (
	"sync"

	af "github.com/LukaKeselj/Agenti/actor-framework"
)

// ── LoggerActor ────────────────────────────────────────────────

// LoggerActor collects metrics, round completions, events, and
// device status messages from all actors and stores them in memory.
type LoggerActor struct {
	af.BaseActor

	mu      sync.Mutex
	metrics []LogMetricsPayload
	rounds  []RoundCompletePayload
	events  []LogEventPayload
	devices []DeviceStatusPayload
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

	case MsgLogMetrics:
		p, ok := msg.Payload.(LogMetricsPayload)
		if !ok {
			return
		}
		l.mu.Lock()
		l.metrics = append(l.metrics, p)
		l.mu.Unlock()

	case MsgLogEvent:
		p, ok := msg.Payload.(LogEventPayload)
		if !ok {
			return
		}
		l.mu.Lock()
		l.events = append(l.events, p)
		l.mu.Unlock()

	case MsgDeviceStatus:
		p, ok := msg.Payload.(DeviceStatusPayload)
		if !ok {
			return
		}
		l.mu.Lock()
		l.devices = append(l.devices, p)
		l.mu.Unlock()
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
