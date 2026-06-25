package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ActorServer is the gRPC server that receives remote actor messages
// and delivers them to local actors.
type ActorServer struct {
	UnimplementedRemoteActorServer
	system *af.ActorSystem
	server *grpc.Server
	lis    net.Listener
	addr   string
}

// NewActorServer creates a new gRPC actor server that will deliver
// incoming remote messages to actors in the given local ActorSystem.
func NewActorServer(system *af.ActorSystem) *ActorServer {
	return &ActorServer{system: system}
}

// Start begins listening on the given address (e.g. ":9000").
func (s *ActorServer) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("remote: cannot listen on %s: %w", addr, err)
	}
	s.lis = lis
	s.addr = lis.Addr().String() // actual address (resolves :0 to real port)

	s.server = grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	RegisterRemoteActorServer(s.server, s)

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.system.Log().Error("gRPC server stopped", "addr", addr, "err", err)
		}
	}()

	s.system.Log().Info("gRPC actor server started", "addr", s.addr)
	return nil
}

// Addr returns the address the server is listening on.
func (s *ActorServer) Addr() string { return s.addr }

// Shutdown gracefully stops the gRPC server.
func (s *ActorServer) Shutdown() {
	if s.server != nil {
		s.server.GracefulStop()
		s.system.Log().Info("gRPC actor server stopped")
	}
}

// ── gRPC handlers ──────────────────────────────────────────────

// Tell delivers a fire-and-forget message to the target actor.
func (s *ActorServer) Tell(_ context.Context, req *TellRequest) (*TellResponse, error) {
	ref, err := s.system.Lookup(af.ActorID(req.ActorId))
	if err != nil {
		s.system.Log().Warn("remote Tell target not found",
			"actor_id", req.ActorId, "msg_type", req.MsgType)
		return &TellResponse{}, nil // fire-and-forget: swallow lookup errors
	}

	msg := af.Message{
		MsgType:   af.MessageType(req.MsgType),
		Payload:   decodePayload(req.PayloadJson, req.PayloadType),
		Sender:    af.ActorID(req.Sender),
		Timestamp: time.Unix(0, req.TimestampNanos),
	}
	ref.Tell(msg)
	return &TellResponse{}, nil
}

// Ask sends a message to the target actor and waits for a reply.
func (s *ActorServer) Ask(ctx context.Context, req *AskRequest) (*AskResponse, error) {
	ref, err := s.system.Lookup(af.ActorID(req.ActorId))
	if err != nil {
		return &AskResponse{
			Success: false,
			Error:   fmt.Sprintf("actor %q not found", req.ActorId),
		}, nil
	}

	msg := af.Message{
		MsgType:   af.MessageType(req.MsgType),
		Payload:   decodePayload(req.PayloadJson, req.PayloadType),
		Sender:    af.ActorID(req.Sender),
		Timestamp: time.Unix(0, req.TimestampNanos),
	}

	askCtx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutMs)*time.Millisecond)
	defer cancel()

	reply, err := ref.Ask(askCtx, msg)
	if err != nil {
		return &AskResponse{Success: false, Error: err.Error()}, nil
	}

	replyJSON, _ := json.Marshal(reply.Payload)
	return &AskResponse{
		Success:     true,
		PayloadJson: replyJSON,
		PayloadType: fmt.Sprintf("%T", reply.Payload),
	}, nil
}

// Stop gracefully stops the target actor.
func (s *ActorServer) Stop(_ context.Context, req *StopRequest) (*StopResponse, error) {
	ref, err := s.system.Lookup(af.ActorID(req.ActorId))
	if err != nil {
		s.system.Log().Warn("remote Stop target not found", "actor_id", req.ActorId)
		return &StopResponse{}, nil
	}
	ref.Stop()
	return &StopResponse{}, nil
}
