package actors

import (
	"fmt"
	"sync"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/smart-home/model"
	"github.com/LukaKeselj/Agenti/smart-home/persistence"
)

// ── CoordinatorActor ───────────────────────────────────────────

// CoordinatorActor manages federated learning rounds.  It sends
// StartTraining to all registered sensors, collects ModelUpdate
// replies, performs FedAvg aggregation (stub in Phase 4), and
// distributes the global model.
type CoordinatorActor struct {
	af.BaseActor

	mu    sync.Mutex
	round int

	sensors      map[string]sensorInfo        // sensorID → info
	pendingRound int                          // round currently waiting for updates
	pendingUpdates map[string]ModelUpdatePayload // sensorID → update (for current round)

	globalWeights []float64
	numWeights    int
	numEpochs     int
	learningRate  float64
	roundStart    time.Time // set at the beginning of each FL round for ElapsedMs

	loggerRef af.ActorID
	deviceRef af.ActorID
	persister *persistence.Persister

	sensorRemoteRefs map[string]af.ActorRef
	onRemoteSensor   func(sensorID, addr string) // callback to create remote ref
}

type sensorInfo struct {
	RoomID     string
	NumSamples int
}

type sensorInfoData struct {
	SensorID   string `json:"sensor_id"`
	RoomID     string `json:"room_id"`
	NumSamples int    `json:"num_samples"`
}

type coordinatorSavedState struct {
	Round         int               `json:"round"`
	GlobalWeights []float64         `json:"global_weights"`
	Sensors       []sensorInfoData  `json:"sensors"`
}

// NewCoordinatorActor creates a new coordinator.
// loggerID and deviceID are the ActorIDs of LoggerActor and DeviceControllerActor.
// numWeights is the number of weights in the MLP model (e.g. 75 for 5→8→3).
func NewCoordinatorActor(id af.ActorID, loggerID, deviceID af.ActorID, numWeights int) *CoordinatorActor {
	return &CoordinatorActor{
		BaseActor:        af.NewBaseActor(id),
		sensors:          make(map[string]sensorInfo),
		pendingUpdates:   make(map[string]ModelUpdatePayload),
		sensorRemoteRefs: make(map[string]af.ActorRef),
		loggerRef:        loggerID,
		deviceRef:        deviceID,
		numWeights:       numWeights,
		numEpochs:        5,
		learningRate:     0.01,
	}
}

// SetPersister attaches a persister for state restoration and saving.
func (c *CoordinatorActor) SetPersister(p *persistence.Persister) {
	c.persister = p
}

func (c *CoordinatorActor) OnPreStart(ctx af.ActorContext) error {
	if c.persister == nil {
		return nil
	}
	var state coordinatorSavedState
	if err := c.persister.Load(string(c.ID()), &state); err != nil {
		return nil
	}
	c.mu.Lock()
	c.round = state.Round
	if state.GlobalWeights != nil {
		c.globalWeights = state.GlobalWeights
	}
	c.sensors = make(map[string]sensorInfo, len(state.Sensors))
	for _, s := range state.Sensors {
		c.sensors[s.SensorID] = sensorInfo{RoomID: s.RoomID, NumSamples: s.NumSamples}
	}
	c.mu.Unlock()
	ctx.Log().Info("restored coordinator state", "round", state.Round, "sensors", len(state.Sensors))
	return nil
}

func (c *CoordinatorActor) persistState() {
	if c.persister == nil {
		return
	}
	c.mu.Lock()
	state := coordinatorSavedState{
		Round:         c.round,
		GlobalWeights: c.globalWeights,
	}
	for id, info := range c.sensors {
		state.Sensors = append(state.Sensors, sensorInfoData{
			SensorID:   id,
			RoomID:     info.RoomID,
			NumSamples: info.NumSamples,
		})
	}
	c.mu.Unlock()
	_ = c.persister.Save(string(c.ID()), state)
}

// OnRemoteSensor sets a callback invoked when a sensor registers with a
// non-empty Address field. The callback should create a RemoteActorRef
// and call SetSensorRemoteRef on the coordinator.
func (c *CoordinatorActor) OnRemoteSensor(fn func(sensorID, addr string)) {
	c.onRemoteSensor = fn
}

// SetSensorRemoteRef registers a remote sensor so the coordinator can
// send messages to it via gRPC instead of local Lookup.
func (c *CoordinatorActor) SetSensorRemoteRef(id string, ref af.ActorRef) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sensorRemoteRefs[id] = ref
}

// SetTrainingConfig overrides the default epochs and learning rate used in FL rounds.
func (c *CoordinatorActor) SetTrainingConfig(epochs int, lr float64) {
	c.mu.Lock()
	c.numEpochs = epochs
	c.learningRate = lr
	c.mu.Unlock()
}

func (c *CoordinatorActor) Receive(ctx af.ActorContext, msg af.Message) {
	switch msg.MsgType {
	case MsgRegisterSensor:
		c.handleRegisterSensor(msg)
	case MsgStartRound:
		c.handleStartRound(ctx)
	case MsgModelUpdate:
		c.handleModelUpdate(ctx, msg)
	case MsgRequestStatus:
		c.handleRequestStatus(ctx, msg)
	}
}

// ── Register sensor ────────────────────────────────────────────

func (c *CoordinatorActor) handleRegisterSensor(msg af.Message) {
	p, ok := castPayload[RegisterSensorPayload](msg.Payload)
	if !ok {
		return
	}
	c.mu.Lock()
	c.sensors[p.SensorID] = sensorInfo{RoomID: p.RoomID, NumSamples: p.NumSamples}
	// Initialise global weights on first registration.
	if c.globalWeights == nil {
		c.globalWeights = make([]float64, c.numWeights)
		for i := range c.globalWeights {
			c.globalWeights[i] = 0.01
		}
	}
	c.mu.Unlock()

	// If sensor provided a remote address, invoke callback to create remote ref.
	if p.Address != "" && c.onRemoteSensor != nil {
		c.onRemoteSensor(p.SensorID, p.Address)
	}
}

// ── Request status ─────────────────────────────────────────────

func (c *CoordinatorActor) handleRequestStatus(ctx af.ActorContext, msg af.Message) {
	c.mu.Lock()
	round := c.round
	sensorCount := len(c.sensors)
	c.mu.Unlock()

	ctx.Log().Info("status requested",
		"round", round,
		"sensors", sensorCount,
	)

	// If the message has a Sender, reply with StatusResponsePayload.
	if msg.Sender != "" {
		if ref, err := ctx.System().Lookup(msg.Sender); err == nil {
			ref.Tell(af.Message{
				MsgType: "smart_home.status_response",
				Payload: StatusResponsePayload{
					Round:       round,
					SensorCount: sensorCount,
				},
			})
		}
	}
}

// ── Start round ────────────────────────────────────────────────

func (c *CoordinatorActor) handleStartRound(ctx af.ActorContext) {
	c.mu.Lock()
	c.round++
	round := c.round
	sensorIDs := make([]string, 0, len(c.sensors))
	for id := range c.sensors {
		sensorIDs = append(sensorIDs, id)
	}
	weights := copyWeights(c.globalWeights)
	c.pendingRound = round
	c.pendingUpdates = make(map[string]ModelUpdatePayload)
	c.roundStart = time.Now()
	c.mu.Unlock()

	if len(sensorIDs) == 0 {
		ctx.Log().Warn("no sensors registered, cannot start round")
		return
	}

	ctx.Log().Info("starting round", "round", round, "sensors", len(sensorIDs))

	c.mu.Lock()
	epochs := c.numEpochs
	lr := c.learningRate
	c.mu.Unlock()

	for _, id := range sensorIDs {
		ref := c.lookupRef(ctx, id)
		if ref == nil {
			ctx.Log().Warn("sensor not found", "sensor_id", id)
			continue
		}
		ref.Tell(af.Message{
			MsgType: MsgStartTraining,
			Payload: StartTrainingPayload{
				RoundID:      round,
				Weights:      weights,
				LearningRate: lr,
				Epochs:       epochs,
			},
			Sender: ctx.Self().ID(),
		})
	}
}

// ── Collect update ─────────────────────────────────────────────

func (c *CoordinatorActor) handleModelUpdate(ctx af.ActorContext, msg af.Message) {
	p, ok := castPayload[ModelUpdatePayload](msg.Payload)
	if !ok {
		ctx.Log().Warn("model update payload type mismatch",
			"payload_type", fmt.Sprintf("%T", msg.Payload))
		return
	}

	c.mu.Lock()
	if p.RoundID != c.pendingRound {
		c.mu.Unlock()
		ctx.Log().Warn("ignoring update for stale round",
			"received_round", p.RoundID, "expected_round", c.pendingRound)
		return
	}
	c.pendingUpdates[p.SensorID] = p
	received := len(c.pendingUpdates)
	total := len(c.sensors)
	c.mu.Unlock()

	ctx.Log().Info("model update received",
		"sensor", p.SensorID, "round", p.RoundID, "received", received, "total", total)

	if received >= total {
		c.aggregateAndFinalise(ctx)
	}
}

// ── Aggregation (FedAvg stub) ──────────────────────────────────

func (c *CoordinatorActor) aggregateAndFinalise(ctx af.ActorContext) {
	c.mu.Lock()
	round := c.round
	updates := make([]ModelUpdatePayload, 0, len(c.pendingUpdates))
	for _, u := range c.pendingUpdates {
		updates = append(updates, u)
	}
	// Collect all registered sensor IDs so GlobalModelUpdate reaches every sensor,
	// including any that missed this round (e.g. crashed and restarted).
	allSensorIDs := make([]string, 0, len(c.sensors))
	for id := range c.sensors {
		allSensorIDs = append(allSensorIDs, id)
	}
	totalSamples := 0
	for _, u := range updates {
		totalSamples += u.NumSamples
	}
	if totalSamples == 0 || len(c.globalWeights) == 0 {
		c.mu.Unlock()
		return
	}

	clientWeights := make([][]float64, len(updates))
	numSamples := make([]int, len(updates))
	for i, u := range updates {
		clientWeights[i] = u.Weights
		numSamples[i] = u.NumSamples
	}

	aggregated := model.FedAvg(clientWeights, numSamples)
	avgLoss := 0.0
	for _, u := range updates {
		ratio := float64(u.NumSamples) / float64(totalSamples)
		avgLoss += u.Loss * ratio
	}
	c.globalWeights = aggregated
	elapsed := time.Since(c.roundStart).Milliseconds()
	c.mu.Unlock()

	// Send GlobalModelUpdate to ALL registered sensors, not just round responders.
	for _, id := range allSensorIDs {
		ref := c.lookupRef(ctx, id)
		if ref == nil {
			continue
		}
		ref.Tell(af.Message{
			MsgType: MsgGlobalModelUpdate,
			Payload: GlobalModelUpdatePayload{
				RoundID:       round,
				GlobalWeights: aggregated,
				GlobalLoss:    avgLoss,
			},
			Sender: ctx.Self().ID(),
		})
	}

	// Notify Logger.
	if c.loggerRef != "" {
		if logRef, err := ctx.System().Lookup(c.loggerRef); err == nil {
			logRef.Tell(af.Message{
				MsgType: MsgRoundComplete,
				Payload: RoundCompletePayload{
					RoundID:    round,
					GlobalLoss: avgLoss,
					NumClients: len(updates),
					ElapsedMs:  elapsed,
				},
			})
		}
	}

	// Send AdjustEnvironment using the global model to infer optimal conditions.
	// A representative daytime-occupied reading is used as the input vector
	// (normalised to [0,1] matching the training pipeline in data/dataset.go).
	if c.deviceRef != "" {
		if devRef, err := ctx.System().Lookup(c.deviceRef); err == nil {
			mlp := model.NewMLP(5, 8, 3)
			mlp.SetWeights(aggregated)
			// Feature vector: temp=24°C, humidity=50%, light=500lx, presence=1, hour=14
			sampleInput := []float64{
				(24.0 - 15) / 20,   // temp → [0,1]
				(50.0 - 20) / 60,   // humidity → [0,1]
				(500.0 - 50) / 950, // light → [0,1]
				1.0,                 // presence: occupied
				14.0 / 23,           // hour: 14:00
			}
			pred := mlp.Predict(sampleInput)
			// De-normalise back to physical units (inverse of dataset.go targets).
			targetTemp := clampF(pred[0]*10+18, 18, 28)   // [0,1] → [18, 28] °C
			targetHum := clampF(pred[1]*40+30, 30, 70)    // [0,1] → [30, 70] %
			targetLight := clampF(pred[2]*300+50, 50, 350) // [0,1] → [50, 350] lx

			devRef.Tell(af.Message{
				MsgType: MsgAdjustEnvironment,
				Payload: AdjustEnvironmentPayload{
					RoomID:      "all",
					TargetTemp:  targetTemp,
					TargetHum:   targetHum,
					TargetLight: targetLight,
				},
			})
		}
	}

	ctx.Log().Info("round complete", "round", round, "clients", len(updates), "avg_loss", avgLoss)

	c.persistState()
}

// ── helpers ────────────────────────────────────────────────────

func (c *CoordinatorActor) RoundID() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.round
}

func (c *CoordinatorActor) SensorCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sensors)
}

// Weights returns a copy of the current global weights.
func (c *CoordinatorActor) Weights() []float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyWeights(c.globalWeights)
}

// lookupRef returns an ActorRef for the given sensor ID, preferring a
// remote ref if one has been registered, otherwise falling back to
// a local Lookup.
func (c *CoordinatorActor) lookupRef(ctx af.ActorContext, id string) af.ActorRef {
	c.mu.Lock()
	if r, ok := c.sensorRemoteRefs[id]; ok {
		c.mu.Unlock()
		return r
	}
	c.mu.Unlock()
	ref, err := ctx.System().Lookup(af.ActorID(id))
	if err != nil {
		return nil
	}
	return ref
}

func clampF(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// StartRound triggers a new FL round (can be called from outside).
func StartRound(ref af.ActorRef) {
	ref.Tell(af.Message{MsgType: MsgStartRound, Payload: StartRoundPayload{}})
}

// RegisterSensor registers a sensor with the coordinator.
// The address parameter is the gRPC address for remote sensors (empty = local).
func RegisterSensor(ref af.ActorRef, sensorID, roomID string, numSamples int, address ...string) {
	addr := ""
	if len(address) > 0 {
		addr = address[0]
	}
	ref.Tell(af.Message{
		MsgType: MsgRegisterSensor,
		Payload: RegisterSensorPayload{
			SensorID:   sensorID,
			RoomID:     roomID,
			NumSamples: numSamples,
			Address:    addr,
		},
	})
}


