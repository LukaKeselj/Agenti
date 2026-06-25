package model_test

import (
	"testing"

	"github.com/LukaKeselj/Agenti/smart-home/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMLP_PredictShape(t *testing.T) {
	m := model.NewMLP(5, 8, 3)
	out := m.Predict([]float64{22.0, 50.0, 300.0, 1.0, 14.0})
	require.Len(t, out, 3)
}

func TestMLP_TrainReducesLoss(t *testing.T) {
	m := model.NewMLP(2, 4, 1)

	input := []float64{0.0, 1.0}
	target := []float64{1.0}

	loss1 := m.Train(input, target, 0.1)
	for i := 0; i < 500; i++ {
		m.Train(input, target, 0.1)
	}
	loss2 := m.Train(input, target, 0.1)

	require.Less(t, loss2, loss1, "loss should decrease: %.6f → %.6f", loss1, loss2)

	pred := m.Predict(input)
	require.Greater(t, pred[0], 0.5, "expected prediction near 1.0, got %.4f", pred[0])
}

func TestMLP_WeightsRoundtrip(t *testing.T) {
	m := model.NewMLP(5, 10, 3)
	w1 := m.Weights()

	m2 := model.NewMLP(5, 10, 3)
	m2.SetWeights(w1)
	w2 := m2.Weights()

	require.Len(t, w2, len(w1))
	for i := range w1 {
		assert.Equal(t, w1[i], w2[i], "weight %d", i)
	}
}

func TestMLP_NumWeights(t *testing.T) {
	m := model.NewMLP(5, 8, 3)
	require.Equal(t, 75, m.NumWeights())
}

func TestFedAvg_Basic(t *testing.T) {
	c1 := []float64{1.0, 2.0}
	c2 := []float64{3.0, 4.0}
	clients := [][]float64{c1, c2}
	samples := []int{100, 300}

	avg := model.FedAvg(clients, samples)
	require.Equal(t, []float64{2.5, 3.5}, avg)
}

func TestFedAvg_EqualWeights(t *testing.T) {
	c1 := []float64{10.0, 20.0}
	c2 := []float64{10.0, 20.0}
	clients := [][]float64{c1, c2}
	samples := []int{100, 100}

	avg := model.FedAvg(clients, samples)
	require.Equal(t, []float64{10.0, 20.0}, avg)
}

func TestFedAvg_Empty(t *testing.T) {
	require.Nil(t, model.FedAvg(nil, nil))
}

func TestMLP_TrainMultiOutput(t *testing.T) {
	m := model.NewMLP(3, 6, 2)
	input := []float64{0.5, -0.2, 0.8}
	target := []float64{0.9, 0.1}

	loss1 := m.Train(input, target, 0.2)
	for i := 0; i < 1000; i++ {
		m.Train(input, target, 0.2)
	}
	loss2 := m.Train(input, target, 0.2)

	require.Less(t, loss2, loss1, "multi-output loss should decrease: %.6f → %.6f", loss1, loss2)

	pred := m.Predict(input)
	assert.Greater(t, pred[0], 0.6, "expected first output near 0.9")
	assert.Less(t, pred[1], 0.4, "expected second output near 0.1")
}
