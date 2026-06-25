package evaluation_test

import (
	"testing"

	"github.com/LukaKeselj/Agenti/smart-home/evaluation"
	"github.com/stretchr/testify/require"
)

func TestCalculate_Perfect(t *testing.T) {
	actual := []float64{1.0, 2.0, 3.0}
	predicted := []float64{1.0, 2.0, 3.0}
	m := evaluation.Calculate(actual, predicted)
	require.Equal(t, 0.0, m.MSE)
	require.Equal(t, 0.0, m.RMSE)
	require.Equal(t, 0.0, m.MAE)
	require.Equal(t, 1.0, m.R2)
}

func TestCalculate_Constant(t *testing.T) {
	actual := []float64{1.0, 2.0, 3.0}
	predicted := []float64{2.0, 2.0, 2.0}
	m := evaluation.Calculate(actual, predicted)
	require.Greater(t, m.MSE, 0.0)
	require.Greater(t, m.RMSE, 0.0)
	require.Greater(t, m.MAE, 0.0)
}

func TestCalculate_R2Bounds(t *testing.T) {
	actual := []float64{0.0, 1.0, 2.0, 3.0}
	predicted := []float64{0.0, 1.0, 2.0, 3.0}
	m := evaluation.Calculate(actual, predicted)
	require.Equal(t, 1.0, m.R2)
}

func TestCalculate_Empty(t *testing.T) {
	m := evaluation.Calculate(nil, nil)
	require.Equal(t, 0.0, m.MSE)
	require.Equal(t, 0.0, m.RMSE)
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
	require.Equal(t, 1.0, m.MSE)
}
