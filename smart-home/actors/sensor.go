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

// Receive is the idle behavior – the actor's default message handler.
// State machine: Idle ──StartTraining──▶ Training ──TrainingComplete──▶ SendingResults ──Done──▶ Idle
func (s *SensorActor) Receive(ctx af.ActorContext, msg af.Message) {
	switch msg.MsgType {
	case MsgStartTraining:
		s.handleStartTraining(ctx, msg)
	case MsgGlobalModelUpdate:
		s.handleGlobalModelUpdate(msg)
	case af.MsgStatusCheck:
		s.handleStatusCheck(ctx)
	case af.MessageType("crash"):
		panic("simulated crash for demo")
	}
}

// trainingBehavior is active while the background SGD goroutine is running.
// It accepts MsgTrainingComplete from the goroutine and handles heartbeats;
// a duplicate MsgStartTraining is silently dropped.
func (s *SensorActor) trainingBehavior(ctx af.ActorContext, msg af.Message) {
	switch msg.MsgType {
	case MsgTrainingComplete:
		// Training → SendingResults
		s.mu.Lock()
		s.state = "sending"
		s.mu.Unlock()
		ctx.Become(s.sendingResultsBehavior) // push SendingResults on top of Training
		s.processSendResults(ctx, msg)
		ctx.Unbecome() // pop SendingResults
		ctx.Unbecome() // pop Training → Idle
		s.mu.Lock()
		s.state = "idle"
		s.mu.Unlock()
	case MsgGlobalModelUpdate:
		s.handleGlobalModelUpdate(msg)
	case af.MsgStatusCheck:
		s.handleStatusCheck(ctx)
	case MsgStartTraining:
		s.systemLog(ctx, "already training – ignoring duplicate StartTraining")
	case af.MessageType("crash"):
		panic("simulated crash for demo")
	}
}

// sendingResultsBehavior is briefly active while ModelUpdate is being sent to
// the coordinator. Any messages that arrive in this window are queued and will
// be dispatched once the actor returns to idle.
func (s *SensorActor) sendingResultsBehavior(_ af.ActorContext, _ af.Message) {
	// Window is so short that no application messages are expected here;
	// heartbeats and GlobalModelUpdate are handled by the caller after Unbecome.
}

// ── Idle state ─────────────────────────────────────────────────

func (s *SensorActor) handleStartTraining(ctx af.ActorContext, msg af.Message) {
	p, ok := castPayload[StartTrainingPayload](msg.Payload)
	if !ok {
		return
	}

	s.mu.Lock()
	s.state = "training"
	s.roundID = p.RoundID
	s.weights = copyWeights(p.Weights)
	s.mu.Unlock()

	s.systemLog(ctx, "training started", "round", p.RoundID)

	// Idle → Training: swap to trainingBehavior until results are sent.
	ctx.Become(s.trainingBehavior)
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

// ── handleStatusCheck ──────────────────────────────────────────

// handleStatusCheck replies to a supervisor heartbeat from any behavior state.
func (s *SensorActor) handleStatusCheck(ctx af.ActorContext) {
	if ref := ctx.Sender(); ref != nil {
		ref.Tell(af.Message{
			MsgType: af.MsgHeartbeat,
			Sender:  ctx.Self().ID(),
		})
	}
}

// ── SendingResults ─────────────────────────────────────────────

// processSendResults runs during the SendingResults behavior: it ships the
// trained weights to the coordinator and persists local state.
// State transitions (Become/Unbecome) are managed by trainingBehavior.
func (s *SensorActor) processSendResults(ctx af.ActorContext, msg af.Message) {
	p, ok := castPayload[TrainingCompletePayload](msg.Payload)
	if !ok {
		return
	}

	s.mu.Lock()
	s.samples = p.NumSamples
	s.mu.Unlock()

	s.systemLog(ctx, "training complete, sending results", "round", s.roundID, "loss", p.Loss)

	// Send ModelUpdate to coordinator (remote ref takes priority).
	coordRef := s.coordinatorRef
	if coordRef == nil {
		var err error
		coordRef, err = ctx.System().Lookup(s.coordinator)
		if err != nil {
			s.systemLog(ctx, "coordinator not found", "id", s.coordinator)
			return // trainingBehavior will Unbecome and reset state to idle
		}
	}

	// Sanitize weights – replace NaN/Inf with zero (Go's json cannot marshal them).
	sanitizedWeights := make([]float64, len(p.Weights))
	for i, w := range p.Weights {
		if math.IsNaN(w) || math.IsInf(w, 0) {
			sanitizedWeights[i] = 0
		} else {
			sanitizedWeights[i] = w
		}
	}
	loss := p.Loss
	if math.IsNaN(loss) || math.IsInf(loss, 0) {
		loss = 0
	}

	coordRef.Tell(af.Message{
		MsgType: MsgModelUpdate,
		Payload: ModelUpdatePayload{
			SensorID:   string(s.ID()),
			RoundID:    s.roundID,
			Weights:    sanitizedWeights,
			NumSamples: p.NumSamples,
			Loss:       loss,
		},
		Sender: ctx.Self().ID(),
	})

	s.persistState()
	// State reset to "idle" is handled by trainingBehavior after Unbecome.
}

// ── Global model update ────────────────────────────────────────

func (s *SensorActor) handleGlobalModelUpdate(msg af.Message) {
	p, ok := castPayload[GlobalModelUpdatePayload](msg.Payload)
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
