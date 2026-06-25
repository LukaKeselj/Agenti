package actors

import (
	"sync"

	af "github.com/LukaKeselj/Agenti/actor-framework"
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
	numFeatures   int

	loggerRef af.ActorID
	deviceRef af.ActorID
}

type sensorInfo struct {
	RoomID     string
	NumSamples int
}

// NewCoordinatorActor creates a new coordinator.
// loggerID and deviceID are the ActorIDs of LoggerActor and DeviceControllerActor.
func NewCoordinatorActor(id af.ActorID, loggerID, deviceID af.ActorID, numFeatures int) *CoordinatorActor {
	return &CoordinatorActor{
		BaseActor:      af.NewBaseActor(id),
		sensors:        make(map[string]sensorInfo),
		pendingUpdates: make(map[string]ModelUpdatePayload),
		loggerRef:      loggerID,
		deviceRef:      deviceID,
		numFeatures:    numFeatures,
	}
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
		c.globalWeights = make([]float64, c.numFeatures)
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
				LearningRate: 0.01,
				Epochs:       5,
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
	// FedAvg: weighted average by num_samples.
	totalSamples := 0
	for _, u := range updates {
		totalSamples += u.NumSamples
	}
	if totalSamples == 0 || len(c.globalWeights) == 0 {
		c.mu.Unlock()
		return
	}

	aggregated := make([]float64, len(c.globalWeights))
	avgLoss := 0.0
	for _, u := range updates {
		ratio := float64(u.NumSamples) / float64(totalSamples)
		for i := range aggregated {
			aggregated[i] += u.Weights[i] * ratio
		}
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
			devRef.Tell(af.Message{
				MsgType: MsgAdjustEnvironment,
				Payload: AdjustEnvironmentPayload{
					RoomID:      "all",
					TargetTemp:  22.0 + aggregated[0],
					TargetHum:   50.0,
					TargetLight: 300.0,
				},
			})
		}
	}

	ctx.Log().Info("round complete", "round", round, "clients", len(updates), "avg_loss", avgLoss)
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


