package actors

import (
	"encoding/json"

	af "github.com/LukaKeselj/Agenti/actor-framework"
)

// castPayload extracts a typed payload from a message, supporting both
// direct type assertions (local actor communication) and generic
// map[string]any payloads that arrive over gRPC when the type registry
// does not have the type registered.
// When a message was sent via ActorRef.Ask, the payload is wrapped in an
// AskEnvelope; castPayload unwraps it transparently so handlers work
// identically for both Tell and Ask callers.
func castPayload[T any](payload any) (T, bool) {
	// Unwrap AskEnvelope so handlers don't need to know about Ask vs Tell.
	if env, ok := payload.(af.AskEnvelope); ok {
		payload = env.Original.Payload
	}
	if p, ok := payload.(T); ok {
		return p, true
	}
	if m, ok := payload.(map[string]any); ok {
		data, err := json.Marshal(m)
		if err != nil {
			var zero T
			return zero, false
		}
		var result T
		if err := json.Unmarshal(data, &result); err != nil {
			var zero T
			return zero, false
		}
		return result, true
	}
	var zero T
	return zero, false
}
