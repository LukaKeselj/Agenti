package actors

import (
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
)

// ── Message types ─────────────────────────────────────────────

const (
	MsgStartTraining      af.MessageType = "smart_home.start_training"
	MsgModelUpdate        af.MessageType = "smart_home.model_update"
	MsgGlobalModelUpdate  af.MessageType = "smart_home.global_model_update"
	MsgRoundComplete      af.MessageType = "smart_home.round_complete"
	MsgAdjustEnvironment  af.MessageType = "smart_home.adjust_environment"
	MsgDeviceStatus       af.MessageType = "smart_home.device_status"
	MsgLogMetrics         af.MessageType = "smart_home.log_metrics"
	MsgLogEvent           af.MessageType = "smart_home.log_event"
	MsgRegisterSensor     af.MessageType = "smart_home.register_sensor"
	MsgStartRound         af.MessageType = "smart_home.start_round"
	MsgTrainingComplete   af.MessageType = "smart_home.training_complete"
	MsgRequestStatus      af.MessageType = "smart_home.request_status"
	MsgEvaluateModel      af.MessageType = "smart_home.evaluate_model"
	MsgEvaluationResult   af.MessageType = "smart_home.evaluation_result"
)

// ── Payload types ─────────────────────────────────────────────

// StartTrainingPayload is sent by Coordinator to each SensorActor
// to begin a local training round.
type StartTrainingPayload struct {
	RoundID      int
	Weights      []float64
	LearningRate float64
	Epochs       int
}

// ModelUpdatePayload is sent by a SensorActor back to the Coordinator
// after local training completes.
type ModelUpdatePayload struct {
	SensorID   string
	RoundID    int
	Weights    []float64
	NumSamples int
	Loss       float64
}

// GlobalModelUpdatePayload is sent by the Coordinator to all SensorActors
// after FedAvg aggregation.
type GlobalModelUpdatePayload struct {
	RoundID       int
	GlobalWeights []float64
	GlobalLoss    float64
}

// RoundCompletePayload is sent by Coordinator to LoggerActor.
type RoundCompletePayload struct {
	RoundID    int
	GlobalLoss float64
	NumClients int
	ElapsedMs  int64
}

// AdjustEnvironmentPayload is sent by Coordinator to DeviceControllerActor
// to set target conditions in a room.
type AdjustEnvironmentPayload struct {
	RoomID      string
	TargetTemp  float64
	TargetHum   float64
	TargetLight float64
}

// DeviceStatusPayload is sent by DeviceControllerActor to LoggerActor.
type DeviceStatusPayload struct {
	RoomID     string
	DeviceType string
	Action     string
	ActorID    string
	Timestamp  time.Time
}

// LogMetricsPayload is sent by any actor to LoggerActor to record metrics.
type LogMetricsPayload struct {
	RoundID int
	MSE     float64
	RMSE    float64
	MAE     float64
	R2Score float64
}

// LogEventPayload is sent by any actor to record a textual event.
type LogEventPayload struct {
	Source string
	Event  string
}

// RegisterSensorPayload is sent by a SensorActor to register with
// the CoordinatorActor.
type RegisterSensorPayload struct {
	SensorID     string
	RoomID       string
	NumSamples   int
	Address      string // gRPC address for remote sensors (empty = local)
}

// TrainingCompletePayload is an internal message a SensorActor sends
// to itself after training finishes in a background goroutine.
type TrainingCompletePayload struct {
	Weights    []float64
	Loss       float64
	NumSamples int
}

// StartRoundPayload is sent to the Coordinator to manually trigger
// a new FL round.
type StartRoundPayload struct{}

// RequestStatusPayload is sent to the Coordinator to request its
// current status (round count, sensor count, etc.).
type RequestStatusPayload struct{}

// EvaluateModelPayload is sent to EvaluatorActor to evaluate global weights.
type EvaluateModelPayload struct {
	RoundID       int
	GlobalWeights []float64
	LoggerID      af.ActorID
}

// EvaluationResultPayload is the reply from EvaluatorActor with computed metrics.
type EvaluationResultPayload struct {
	RoundID int
	MSE     float64
	RMSE    float64
	MAE     float64
	R2Score float64
}

// StatusResponsePayload is the reply to a RequestStatus query.
type StatusResponsePayload struct {
	Round       int
	SensorCount int
}
