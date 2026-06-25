package actors

import (
	"math"
	"sync"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/smart-home/data"
	"github.com/LukaKeselj/Agenti/smart-home/model"
	"github.com/LukaKeselj/Agenti/smart-home/persistence"
)

// ── SensorActor ────────────────────────────────────────────────

// SensorActor simulates an IoT sensor in a room.  It participates
// in federated learning rounds. When trainData is provided, it uses
// a real MLP model; otherwise it falls back to stub training (tests).
type SensorActor struct {
	af.BaseActor

	mu    sync.Mutex
	state string // "idle", "training", "sending"

	roomID   string
	roundID  int
	weights  []float64
	samples  int

	mlp         *model.MLP
	trainData   []data.Sample
	coordinator  af.ActorID
	coordinatorRef af.ActorRef // optional, overrides Lookup for remote coord
	persister   *persistence.Persister
}

type sensorSavedState struct {
	Weights  []float64 `json:"weights"`
	RoundID  int       `json:"round_id"`
	Samples  int       `json:"samples"`
}

func (s *SensorActor) SetPersister(p *persistence.Persister) {
	s.persister = p
}

// SetCoordinatorRef overrides local Lookup for the coordinator,
// allowing the sensor to send messages via a RemoteActorRef.
func (s *SensorActor) SetCoordinatorRef(ref af.ActorRef) {
	s.coordinatorRef = ref
}

func (s *SensorActor) OnPreStart(ctx af.ActorContext) error {
	if s.persister == nil {
		return nil
	}
	var state sensorSavedState
	if err := s.persister.Load(string(s.ID()), &state); err != nil {
		return nil
	}
	s.mu.Lock()
	s.weights = state.Weights
	s.roundID = state.RoundID
	s.samples = state.Samples
	s.mu.Unlock()
	ctx.Log().Info("restored sensor state", "round", state.RoundID, "samples", state.Samples)
	return nil
}

func (s *SensorActor) persistState() {
	if s.persister == nil {
		return
	}
	s.mu.Lock()
	state := sensorSavedState{
		Weights: s.weights,
		RoundID: s.roundID,
		Samples: s.samples,
	}
	s.mu.Unlock()
	_ = s.persister.Save(string(s.ID()), state)
}

// NewSensorActor creates a sensor for the given room.
// Pass trainData to enable real MLP training (nil = stub).
func NewSensorActor(id af.ActorID, roomID string, coordinator af.ActorID, trainData ...[]data.Sample) *SensorActor {
	s := &SensorActor{
		BaseActor:   af.NewBaseActor(id),
		state:       "idle",
		roomID:      roomID,
		coordinator: coordinator,
	}
	if len(trainData) > 0 && len(trainData[0]) > 0 {
		s.trainData = trainData[0]
		s.mlp = model.NewMLP(5, 8, 3)
	}
	return s
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

	go s.simulateTraining(ctx.Self(), p)
}

func (s *SensorActor) simulateTraining(self af.ActorRef, p StartTrainingPayload) {
	if s.mlp != nil && len(s.trainData) > 0 {
		s.realTraining(self, p)
	} else {
		s.stubTraining(self, p)
	}
}

func (s *SensorActor) realTraining(self af.ActorRef, p StartTrainingPayload) {
	s.mlp.SetWeights(p.Weights)

	for epoch := 0; epoch < p.Epochs; epoch++ {
		for _, sample := range s.trainData {
			s.mlp.Train(sample.Features, sample.Target, p.LearningRate)
		}
	}

	loss := computeLoss(s.mlp, s.trainData)
	trained := s.mlp.Weights()
	numSamples := len(s.trainData)

	self.Tell(af.Message{
		MsgType: MsgTrainingComplete,
		Payload: TrainingCompletePayload{
			Weights:    trained,
			Loss:       loss,
			NumSamples: numSamples,
		},
	})
}

func computeLoss(mlp *model.MLP, samples []data.Sample) float64 {
	var total float64
	for _, s := range samples {
		pred := mlp.Predict(s.Features)
		for j := range pred {
			diff := pred[j] - s.Target[j]
			total += diff * diff
		}
	}
	return math.Round(total/float64(len(samples)*3)*1e6) / 1e6
}

func (s *SensorActor) stubTraining(self af.ActorRef, p StartTrainingPayload) {
	delay := time.Duration(50+p.Epochs*10) * time.Millisecond
	time.Sleep(delay)

	numSamples := 80 + (int(time.Now().UnixNano()) % 40)
	trained := copyWeights(p.Weights)
	for i := range trained {
		trained[i] += 0.01 * float64(i%3-1)
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

	// Send ModelUpdate to coordinator (remote ref takes priority).
	coordRef := s.coordinatorRef
	if coordRef == nil {
		var err error
		coordRef, err = ctx.System().Lookup(s.coordinator)
		if err != nil {
			s.systemLog(ctx, "coordinator not found", "id", s.coordinator)
			s.mu.Lock()
			s.state = "idle"
			s.mu.Unlock()
			return
		}
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

	s.persistState()
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
