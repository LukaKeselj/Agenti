package actors

import (
	"sync"

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

	loggerRef af.ActorID
	deviceRef af.ActorID
	persister *persistence.Persister
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
		BaseActor:     af.NewBaseActor(id),
		sensors:       make(map[string]sensorInfo),
		pendingUpdates: make(map[string]ModelUpdatePayload),
		loggerRef:     loggerID,
		deviceRef:     deviceID,
		numWeights:    numWeights,
		numEpochs:     5,
		learningRate:  0.01,
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
	}
}

// ── Register sensor ────────────────────────────────────────────

func (c *CoordinatorActor) handleRegisterSensor(msg af.Message) {
	p, ok := msg.Payload.(RegisterSensorPayload)
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
		ref, err := ctx.System().Lookup(af.ActorID(id))
		if err != nil {
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
	p, ok := msg.Payload.(ModelUpdatePayload)
	if !ok {
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
	c.mu.Unlock()

	// Send GlobalModelUpdate to all sensors.
	for _, u := range updates {
		ref, err := ctx.System().Lookup(af.ActorID(u.SensorID))
		if err != nil {
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
					ElapsedMs:  100,
				},
			})
		}
	}

	// Send AdjustEnvironment based on predictions (stub).
	if c.deviceRef != "" {
		if devRef, err := ctx.System().Lookup(c.deviceRef); err == nil {
			temp := 22.0 + aggregated[0]*0.1
			if temp < 18.0 {
				temp = 18.0
			} else if temp > 28.0 {
				temp = 28.0
			}
			devRef.Tell(af.Message{
				MsgType: MsgAdjustEnvironment,
				Payload: AdjustEnvironmentPayload{
					RoomID:      "all",
					TargetTemp:  temp,
					TargetHum:   50.0,
					TargetLight: 300.0,
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

// StartRound triggers a new FL round (can be called from outside).
func StartRound(ref af.ActorRef) {
	ref.Tell(af.Message{MsgType: MsgStartRound, Payload: StartRoundPayload{}})
}

// RegisterSensor registers a sensor with the coordinator.
func RegisterSensor(ref af.ActorRef, sensorID, roomID string, numSamples int) {
	ref.Tell(af.Message{
		MsgType: MsgRegisterSensor,
		Payload: RegisterSensorPayload{
			SensorID:   sensorID,
			RoomID:     roomID,
			NumSamples: numSamples,
		},
	})
}


