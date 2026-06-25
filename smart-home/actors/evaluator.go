package actors

import (
	"sync"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/smart-home/data"
	"github.com/LukaKeselj/Agenti/smart-home/evaluation"
	"github.com/LukaKeselj/Agenti/smart-home/model"
)

type EvaluatorActor struct {
	af.BaseActor

	mu       sync.Mutex
	valSet   []data.Sample
	results  []evaluation.RoundResult
}

func NewEvaluatorActor(id af.ActorID, valSet []data.Sample) *EvaluatorActor {
	return &EvaluatorActor{
		BaseActor: af.NewBaseActor(id),
		valSet:    valSet,
	}
}

func (e *EvaluatorActor) Receive(ctx af.ActorContext, msg af.Message) {
	switch msg.MsgType {
	case MsgEvaluateModel:
		e.handleEvaluateModel(ctx, msg)
	}
}

func (e *EvaluatorActor) handleEvaluateModel(ctx af.ActorContext, msg af.Message) {
	p, ok := castPayload[EvaluateModelPayload](msg.Payload)
	if !ok {
		return
	}

	mlp := model.NewMLP(5, 8, 3)
	mlp.SetWeights(p.GlobalWeights)

	var actuals, predictions []float64
	for _, s := range e.valSet {
		pred := mlp.Predict(s.Features)
		actuals = append(actuals, s.Target...)
		predictions = append(predictions, pred...)
	}

	metrics := evaluation.Calculate(actuals, predictions)

	e.mu.Lock()
	e.results = append(e.results, evaluation.RoundResult{Round: p.RoundID, Metrics: metrics})
	e.mu.Unlock()

	ctx.Log().Info("evaluation complete", "round", p.RoundID, "mse", metrics.MSE, "r2", metrics.R2)

	// Send LogMetrics to logger.
	if p.LoggerID != "" {
		if logRef, err := ctx.System().Lookup(p.LoggerID); err == nil {
			logRef.Tell(af.Message{
				MsgType: MsgLogMetrics,
				Payload: LogMetricsPayload{
					RoundID: p.RoundID,
					MSE:     metrics.MSE,
					RMSE:    metrics.RMSE,
					MAE:     metrics.MAE,
					R2Score: metrics.R2,
				},
			})
		}
	}

	// Reply to sender if it's an Ask.
	if sender, ok := af.IsAsk(msg); ok {
		sender.ReplyCh <- af.Message{
			Payload: EvaluationResultPayload{
				RoundID: p.RoundID,
				MSE:     metrics.MSE,
				RMSE:    metrics.RMSE,
				MAE:     metrics.MAE,
				R2Score: metrics.R2,
			},
		}
	}
}

func (e *EvaluatorActor) Results() []evaluation.RoundResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]evaluation.RoundResult, len(e.results))
	copy(out, e.results)
	return out
}
