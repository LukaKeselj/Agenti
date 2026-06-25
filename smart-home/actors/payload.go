package actors

import (
	"encoding/json"
)

// castPayload extracts a typed payload from a message, supporting both
// direct type assertions (local actor communication) and generic
// map[string]any payloads that arrive over gRPC when the type registry
// does not have the type registered.
func castPayload[T any](payload any) (T, bool) {
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
