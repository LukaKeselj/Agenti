package actors

import "github.com/LukaKeselj/Agenti/actor-framework/remote"

func init() {
	remote.RegisterPayloadType(StartTrainingPayload{})
	remote.RegisterPayloadType(ModelUpdatePayload{})
	remote.RegisterPayloadType(GlobalModelUpdatePayload{})
	remote.RegisterPayloadType(RoundCompletePayload{})
	remote.RegisterPayloadType(AdjustEnvironmentPayload{})
	remote.RegisterPayloadType(DeviceStatusPayload{})
	remote.RegisterPayloadType(LogMetricsPayload{})
	remote.RegisterPayloadType(LogEventPayload{})
	remote.RegisterPayloadType(RegisterSensorPayload{})
	remote.RegisterPayloadType(StartRoundPayload{})
	remote.RegisterPayloadType(TrainingCompletePayload{})
	remote.RegisterPayloadType(RequestStatusPayload{})
	remote.RegisterPayloadType(StatusResponsePayload{})
	remote.RegisterPayloadType(EvaluateModelPayload{})
	remote.RegisterPayloadType(EvaluationResultPayload{})
}
