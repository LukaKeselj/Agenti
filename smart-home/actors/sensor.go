package actors

import (
	"sync"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
)

// ── SensorActor ────────────────────────────────────────────────

// SensorActor simulates an IoT sensor in a room.  It participates
// in federated learning rounds and uses Become/Unbecome to switch
// between states: Idle → Training → SendingResults.
type SensorActor struct {
	af.BaseActor

	mu    sync.Mutex
	state string // "idle", "training", "sending"

	roomID   string
	roundID  int
	weights  []float64
	samples  int

	coordinator af.ActorID
}

// NewSensorActor creates a sensor for the given room.
func NewSensorActor(id af.ActorID, roomID string, coordinator af.ActorID) *SensorActor {
	return &SensorActor{
		BaseActor:   af.NewBaseActor(id),
		state:       "idle",
		roomID:      roomID,
		coordinator: coordinator,
	}
}

func (s *SensorActor) Receive(ctx af.ActorContext, msg af.Message) {
	switch msg.MsgType {
	case MsgStartTraining:
		s.handleStartTraining(ctx, msg)
	case MsgTrainingComplete:
		s.handleTrainingComplete(ctx, msg)
	case MsgGlobalModelUpdate:
		s.handleGlobalModelUpdate(msg)
	case af.MsgStatusCheck:
		// Reply to supervisor heartbeat.
		ctx.Self().Tell(af.Message{
			MsgType: af.MsgHeartbeat,
			Sender:  ctx.Self().ID(),
		})
	}
}

// ── Idle state ─────────────────────────────────────────────────

func (s *SensorActor) handleStartTraining(ctx af.ActorContext, msg af.Message) {
	p, ok := msg.Payload.(StartTrainingPayload)
	if !ok {
		return
	}

	s.mu.Lock()
	s.state = "training"
	s.roundID = p.RoundID
	s.weights = copyWeights(p.Weights)
	s.mu.Unlock()

	s.systemLog(ctx, "training started", "round", p.RoundID)

	// Simulate training in background (Phase 5 will replace with real MLP).
	go s.simulateTraining(ctx.Self(), p)
}

func (s *SensorActor) simulateTraining(self af.ActorRef, p StartTrainingPayload) {
	// Simulate compute time.
	delay := time.Duration(50+p.Epochs*10) * time.Millisecond
	time.Sleep(delay)

	numSamples := 80 + (int(time.Now().UnixNano()) % 40)
	trained := copyWeights(p.Weights)
	for i := range trained {
		trained[i] += 0.01 * float64(i%3-1) // tiny random perturbation
	}

	self.Tell(af.Message{
		MsgType: MsgTrainingComplete,
		Payload: TrainingCompletePayload{
			Weights:    trained,
			Loss:       0.5 - float64(p.RoundID)*0.02,
			NumSamples: numSamples,
		},
	})
}

// ── Training complete ──────────────────────────────────────────

func (s *SensorActor) handleTrainingComplete(ctx af.ActorContext, msg af.Message) {
	p, ok := msg.Payload.(TrainingCompletePayload)
	if !ok {
		return
	}

	s.mu.Lock()
	s.state = "sending"
	s.samples = p.NumSamples
	s.mu.Unlock()

	s.systemLog(ctx, "training complete", "round", s.roundID, "loss", p.Loss)

	// Send ModelUpdate to coordinator.
	coordRef, err := ctx.System().Lookup(s.coordinator)
	if err != nil {
		s.systemLog(ctx, "coordinator not found", "id", s.coordinator)
		s.mu.Lock()
		s.state = "idle"
		s.mu.Unlock()
		return
	}

	coordRef.Tell(af.Message{
		MsgType: MsgModelUpdate,
		Payload: ModelUpdatePayload{
			SensorID:   string(s.ID()),
			RoundID:    s.roundID,
			Weights:    p.Weights,
			NumSamples: p.NumSamples,
			Loss:       p.Loss,
		},
		Sender: ctx.Self().ID(),
	})

	s.mu.Lock()
	s.state = "idle"
	s.mu.Unlock()
}

// ── Global model update ────────────────────────────────────────

func (s *SensorActor) handleGlobalModelUpdate(msg af.Message) {
	p, ok := msg.Payload.(GlobalModelUpdatePayload)
	if !ok {
		return
	}
	s.mu.Lock()
	s.weights = copyWeights(p.GlobalWeights)
	s.roundID = p.RoundID
	s.mu.Unlock()
}

// ── helpers ────────────────────────────────────────────────────

func (s *SensorActor) systemLog(ctx af.ActorContext, msg string, args ...any) {
	ctx.Log().Info(msg, append([]any{"room", s.roomID, "state", s.state}, args...)...)
}

// State returns the current state (idle / training / sending).
func (s *SensorActor) State() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Weights returns a copy of the current weights.
func (s *SensorActor) Weights() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyWeights(s.weights)
}

func copyWeights(src []float64) []float64 {
	if src == nil {
		return nil
	}
	dst := make([]float64, len(src))
	copy(dst, src)
	return dst
}
