package model_test

import (
	"testing"

	"github.com/LukaKeselj/Agenti/smart-home/model"
)

func TestMLP_PredictShape(t *testing.T) {
	m := model.NewMLP(5, 8, 3)
	out := m.Predict([]float64{22.0, 50.0, 300.0, 1.0, 14.0})
	if len(out) != 3 {
		t.Fatalf("expected 3 outputs, got %d", len(out))
	}
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

	if loss2 >= loss1 {
		t.Fatalf("expected loss to decrease: %.6f → %.6f", loss1, loss2)
	}

	pred := m.Predict(input)
	if pred[0] < 0.5 {
		t.Fatalf("expected prediction near 1.0, got %.4f", pred[0])
	}
}

func TestMLP_WeightsRoundtrip(t *testing.T) {
	m := model.NewMLP(5, 10, 3)
	w1 := m.Weights()

	m2 := model.NewMLP(5, 10, 3)
	m2.SetWeights(w1)
	w2 := m2.Weights()

	if len(w1) != len(w2) {
		t.Fatalf("weight count mismatch: %d vs %d", len(w1), len(w2))
	}
	for i := range w1 {
		if w1[i] != w2[i] {
			t.Fatalf("weight %d differs: %.10f vs %.10f", i, w1[i], w2[i])
		}
	}
}

func TestMLP_NumWeights(t *testing.T) {
	// 5 inputs, 8 hidden, 3 outputs
	// W1: 5*8 = 40, B1: 8, W2: 8*3 = 24, B2: 3 → total 75
	m := model.NewMLP(5, 8, 3)
	if n := m.NumWeights(); n != 75 {
		t.Fatalf("expected 75 weights, got %d", n)
	}
}

func TestFedAvg_Basic(t *testing.T) {
	c1 := []float64{1.0, 2.0}
	c2 := []float64{3.0, 4.0}
	clients := [][]float64{c1, c2}
	samples := []int{100, 300} // total 400

	avg := model.FedAvg(clients, samples)
	// weighted: c1*0.25 + c2*0.75
	if avg[0] != 2.5 || avg[1] != 3.5 {
		t.Fatalf("expected [2.5, 3.5], got %v", avg)
	}
}

func TestFedAvg_EqualWeights(t *testing.T) {
	c1 := []float64{10.0, 20.0}
	c2 := []float64{10.0, 20.0}
	clients := [][]float64{c1, c2}
	samples := []int{100, 100}

	avg := model.FedAvg(clients, samples)
	if avg[0] != 10.0 || avg[1] != 20.0 {
		t.Fatalf("expected [10, 20], got %v", avg)
	}
}

func TestFedAvg_Empty(t *testing.T) {
	if avg := model.FedAvg(nil, nil); avg != nil {
		t.Fatal("expected nil for empty input")
	}
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

	if loss2 >= loss1 {
		t.Fatalf("expected multi-output loss to decrease: %.6f → %.6f", loss1, loss2)
	}

	pred := m.Predict(input)
	if pred[0] < 0.6 || pred[1] > 0.4 {
		t.Fatalf("expected pred near [0.9, 0.1], got [%.4f, %.4f]", pred[0], pred[1])
	}
}
