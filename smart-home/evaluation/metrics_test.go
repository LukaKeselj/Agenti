package evaluation_test

import (
	"testing"

	"github.com/LukaKeselj/Agenti/smart-home/evaluation"
)

func TestCalculate_Perfect(t *testing.T) {
	actual := []float64{1.0, 2.0, 3.0}
	predicted := []float64{1.0, 2.0, 3.0}
	m := evaluation.Calculate(actual, predicted)
	if m.MSE != 0 || m.RMSE != 0 || m.MAE != 0 || m.R2 != 1.0 {
		t.Fatalf("expected perfect metrics, got %+v", m)
	}
}

func TestCalculate_Constant(t *testing.T) {
	actual := []float64{1.0, 2.0, 3.0}
	predicted := []float64{2.0, 2.0, 2.0}
	m := evaluation.Calculate(actual, predicted)
	if m.MSE <= 0 || m.RMSE <= 0 || m.MAE <= 0 {
		t.Fatalf("expected positive errors, got %+v", m)
	}
}

func TestCalculate_R2Bounds(t *testing.T) {
	actual := []float64{0.0, 1.0, 2.0, 3.0}
	predicted := []float64{0.0, 1.0, 2.0, 3.0}
	m := evaluation.Calculate(actual, predicted)
	if m.R2 != 1.0 {
		t.Fatalf("expected R²=1.0, got %f", m.R2)
	}
}

func TestCalculate_Empty(t *testing.T) {
	m := evaluation.Calculate(nil, nil)
	if m.MSE != 0 || m.RMSE != 0 {
		t.Fatalf("expected zero metrics for empty input, got %+v", m)
	}
}

func TestPrintConvergenceTable_NoPanic(t *testing.T) {
	evaluation.PrintConvergenceTable(nil)
	evaluation.PrintConvergenceTable([]evaluation.RoundResult{})
	evaluation.PrintConvergenceTable([]evaluation.RoundResult{
		{Round: 1, Metrics: evaluation.Calculate([]float64{1, 2}, []float64{1, 2})},
	})
}

func TestPrintSummary_NoPanic(t *testing.T) {
	m := evaluation.Calculate([]float64{1, 2}, []float64{1.1, 1.9})
	evaluation.PrintSummary(5, m)
}

func TestCalculate_OffByOne(t *testing.T) {
	actual := []float64{10.0, 20.0, 30.0}
	predicted := []float64{11.0, 21.0, 31.0}
	m := evaluation.Calculate(actual, predicted)
	// MSE = ((10-11)^2 + (20-21)^2 + (30-31)^2) / 3 = (1+1+1)/3 = 1
	if m.MSE != 1.0 {
		t.Fatalf("expected MSE=1.0, got %f", m.MSE)
	}
}
