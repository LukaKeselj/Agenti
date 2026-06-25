package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ── Payload type registry ─────────────────────────────────────────
// Remote messages carry payloads as JSON. The type registry lets
// decodePayload reconstruct typed Go structs instead of map[string]any.

var (
	payloadRegistryMu sync.RWMutex
	payloadRegistry   = map[string]func() any{} // typeName → factory returning *T
)

// RegisterPayloadType registers a payload type so that decodePayload can
// reconstruct it as the original Go struct (not a generic map).
//
//	remote.RegisterPayloadType(actors.StartTrainingPayload{})
func RegisterPayloadType(example any) {
	name := fmt.Sprintf("%T", example)
	typ := reflect.TypeOf(example)
	payloadRegistryMu.Lock()
	payloadRegistry[name] = func() any {
		return reflect.New(typ).Interface()
	}
	payloadRegistryMu.Unlock()
}

// RemoteActorRef implements af.ActorRef and forwards every call over gRPC
// to a remote actor living in another process or machine.
//
// The caller interacts with it exactly like a local ActorRef; the remote
// transport is completely transparent.
type RemoteActorRef struct {
	id     af.ActorID
	target string // "host:port" of the remote gRPC endpoint

	mu     sync.Mutex
	conn   *grpc.ClientConn
	client RemoteActorClient
}

// NewRemoteActorRef creates a new RemoteActorRef that points to an actor
// with the given ID on the given remote gRPC endpoint.
//
// The gRPC connection is dialed lazily on the first call.
func NewRemoteActorRef(id af.ActorID, target string) *RemoteActorRef {
	return &RemoteActorRef{id: id, target: target}
}

// ID returns the actor ID this ref points to.
func (r *RemoteActorRef) ID() af.ActorID { return r.id }

// Tell sends a fire-and-forget message to the remote actor.
func (r *RemoteActorRef) Tell(msg af.Message) {
	req := marshalTell(r.id, msg)
	if req == nil {
		slog.Warn("remote Tell: marshalTell returned nil", "target", r.target, "actor", r.id)
		return
	}
	client, err := r.getClient()
	if err != nil {
		slog.Warn("remote Tell: getClient failed", "target", r.target, "actor", r.id, "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.Tell(ctx, req)
	if err != nil {
		slog.Warn("remote Tell failed", "target", r.target, "actor", r.id, "msg_type", msg.MsgType, "err", err)
	}
}

// Ask sends a message and waits for a single reply, or returns an error
// if the remote actor does not reply within the context deadline.
func (r *RemoteActorRef) Ask(ctx context.Context, msg af.Message) (af.Message, error) {
	req := marshalAsk(r.id, msg)

	// Honour caller's deadline.
	deadline, ok := ctx.Deadline()
	if ok {
		req.TimeoutMs = time.Until(deadline).Milliseconds()
		if req.TimeoutMs <= 0 {
			return af.Message{}, ctx.Err()
		}
	} else {
		req.TimeoutMs = 30000 // 30s default
	}

	client, err := r.getClient()
	if err != nil {
		return af.Message{}, fmt.Errorf("remote: cannot dial %s: %w", r.target, err)
	}

	resp, err := client.Ask(ctx, req)
	if err != nil {
		return af.Message{}, fmt.Errorf("remote ask to %s/%s failed: %w", r.target, r.id, err)
	}
	if !resp.Success {
		return af.Message{}, fmt.Errorf("remote ask error: %s", resp.Error)
	}

	return unmarshalReply(resp), nil
}

// Stop sends a graceful stop request to the remote actor.
func (r *RemoteActorRef) Stop() {
	client, err := r.getClient()
	if err != nil {
		return
	}
	_, _ = client.Stop(context.Background(), &StopRequest{ActorId: string(r.id)})
}

// Close tears down the underlying gRPC connection.
func (r *RemoteActorRef) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		err := r.conn.Close()
		r.conn = nil
		r.client = nil
		return err
	}
	return nil
}

// ── internal helpers ────────────────────────────────────────────

func (r *RemoteActorRef) getClient() (RemoteActorClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.client != nil {
		return r.client, nil
	}
	conn, err := grpc.NewClient(r.target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	r.conn = conn
	r.client = NewRemoteActorClient(conn)
	return r.client, nil
}

// ── serialization ───────────────────────────────────────────────

func marshalTell(actorID af.ActorID, msg af.Message) *TellRequest {
	jsonBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		slog.Warn("marshalTell: json.Marshal failed", "actor", actorID, "msg_type", msg.MsgType, "err", err)
	}
	return &TellRequest{
		ActorId:        string(actorID),
		MsgType:        string(msg.MsgType),
		PayloadJson:    jsonBytes,
		PayloadType:    fmt.Sprintf("%T", msg.Payload),
		Sender:         string(msg.Sender),
		TimestampNanos: msg.Timestamp.UnixNano(),
	}
}

func marshalAsk(actorID af.ActorID, msg af.Message) *AskRequest {
	jsonBytes, _ := json.Marshal(msg.Payload)
	return &AskRequest{
		ActorId:        string(actorID),
		MsgType:        string(msg.MsgType),
		PayloadJson:    jsonBytes,
		PayloadType:    fmt.Sprintf("%T", msg.Payload),
		Sender:         string(msg.Sender),
		TimestampNanos: msg.Timestamp.UnixNano(),
	}
}

func unmarshalReply(resp *AskResponse) af.Message {
	return af.Message{
		MsgType:   "",
		Payload:   decodePayload(resp.PayloadJson, resp.PayloadType),
		Timestamp: time.Now(),
	}
}

func decodePayload(data []byte, typeName string) any {
	if len(data) == 0 {
		return nil
	}
	// Try registered type first.
	payloadRegistryMu.RLock()
	factory, ok := payloadRegistry[typeName]
	payloadRegistryMu.RUnlock()
	if ok {
		ptr := factory() // *T
		if err := json.Unmarshal(data, ptr); err == nil {
			return reflect.ValueOf(ptr).Elem().Interface() // T
		}
	}
	// TypeRegistry miss – log the first one and fallback to generic map.
	if !ok {
		registryMissOnce.Do(func() {
			slog.Warn("decodePayload: type not registered, falling back to map",
				"type", typeName)
		})
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return data
	}
	return v
}

var registryMissOnce sync.Once
