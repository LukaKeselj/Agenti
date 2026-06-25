package model

import (
	"math"
	"math/rand"
)

// MLP is a multi-layer perceptron with one hidden layer.
// Architecture: InputDim → HiddenDim (ReLU) → OutputDim (linear).
//
// All weights are stored as flat []float64 so they can be easily
// exchanged between actors for federated averaging.
type MLP struct {
	InputDim  int
	HiddenDim int
	OutputDim int

	W1 []float64 // input → hidden  (InputDim * HiddenDim)
	B1 []float64 // hidden bias     (HiddenDim)
	W2 []float64 // hidden → output (HiddenDim * OutputDim)
	B2 []float64 // output bias     (OutputDim)
}

// NewMLP creates an MLP with the given architecture and
// initialises weights with small random values.
func NewMLP(inputDim, hiddenDim, outputDim int) *MLP {
	m := &MLP{
		InputDim:  inputDim,
		HiddenDim: hiddenDim,
		OutputDim: outputDim,
		W1:        make([]float64, inputDim*hiddenDim),
		B1:        make([]float64, hiddenDim),
		W2:        make([]float64, hiddenDim*outputDim),
		B2:        make([]float64, outputDim),
	}
	// He initialisation.
	std := math.Sqrt(2.0 / float64(inputDim))
	for i := range m.W1 {
		m.W1[i] = rand.NormFloat64() * std
	}
	std2 := math.Sqrt(2.0 / float64(hiddenDim))
	for i := range m.W2 {
		m.W2[i] = rand.NormFloat64() * std2
	}
	return m
}

// ── forward ───────────────────────────────────────────────────

// Predict runs a forward pass through the network.
func (m *MLP) Predict(input []float64) []float64 {
	hidden := m.hidden(input)
	output := make([]float64, m.OutputDim)
	for j := 0; j < m.OutputDim; j++ {
		var sum float64
		for i := 0; i < m.HiddenDim; i++ {
			sum += hidden[i] * m.W2[i*m.OutputDim+j]
		}
		output[j] = sum + m.B2[j]
	}
	return output
}

func (m *MLP) hidden(input []float64) []float64 {
	h := make([]float64, m.HiddenDim)
	for j := 0; j < m.HiddenDim; j++ {
		var sum float64
		for i := 0; i < m.InputDim; i++ {
			sum += input[i] * m.W1[i*m.HiddenDim+j]
		}
		h[j] = relu(sum + m.B1[j])
	}
	return h
}

// ── train (single step) ───────────────────────────────────────

// Train performs one SGD step on a single (input, target) pair
// and returns the MSE loss before the update.
func (m *MLP) Train(input, target []float64, lr float64) float64 {
	// Forward.
	hidden := m.hidden(input)
	output := m.Predict(input)

	// MSE loss.
	loss := 0.0
	for j := 0; j < m.OutputDim; j++ {
		diff := output[j] - target[j]
		loss += diff * diff
	}
	loss /= float64(m.OutputDim)

	// Output layer gradients.
	dOutput := make([]float64, m.OutputDim)
	for j := 0; j < m.OutputDim; j++ {
		dOutput[j] = 2.0 * (output[j] - target[j]) / float64(m.OutputDim)
	}

	// Backprop into hidden layer.
	dHidden := make([]float64, m.HiddenDim)
	for i := 0; i < m.HiddenDim; i++ {
		var sum float64
		for j := 0; j < m.OutputDim; j++ {
			sum += dOutput[j] * m.W2[i*m.OutputDim+j]
		}
		// ReLU derivative.
		if hidden[i] > 0 {
			dHidden[i] = sum
		}
	}

	// Update W2, B2.
	for i := 0; i < m.HiddenDim; i++ {
		for j := 0; j < m.OutputDim; j++ {
			m.W2[i*m.OutputDim+j] -= lr * dOutput[j] * hidden[i]
		}
	}
	for j := 0; j < m.OutputDim; j++ {
		m.B2[j] -= lr * dOutput[j]
	}

	// Update W1, B1.
	for i := 0; i < m.InputDim; i++ {
		for j := 0; j < m.HiddenDim; j++ {
			m.W1[i*m.HiddenDim+j] -= lr * dHidden[j] * input[i]
		}
	}
	for j := 0; j < m.HiddenDim; j++ {
		m.B1[j] -= lr * dHidden[j]
	}

	return loss
}

// ── weights as flat slice (for FedAvg) ─────────────────────────

// Weights returns a flat copy of all parameters.
func (m *MLP) Weights() []float64 {
	total := len(m.W1) + len(m.B1) + len(m.W2) + len(m.B2)
	out := make([]float64, total)
	off := 0
	copy(out[off:], m.W1); off += len(m.W1)
	copy(out[off:], m.B1); off += len(m.B1)
	copy(out[off:], m.W2); off += len(m.W2)
	copy(out[off:], m.B2)
	return out
}

// SetWeights copies a flat weight vector back into the network.
func (m *MLP) SetWeights(w []float64) {
	off := 0
	copy(m.W1, w[off:]); off += len(m.W1)
	copy(m.B1, w[off:]); off += len(m.B1)
	copy(m.W2, w[off:]); off += len(m.W2)
	copy(m.B2, w[off:])
}

// NumWeights returns the total number of scalar parameters.
func (m *MLP) NumWeights() int {
	return len(m.W1) + len(m.B1) + len(m.W2) + len(m.B2)
}

// ── activation ─────────────────────────────────────────────────

func relu(x float64) float64 {
	if x > 0 {
		return x
	}
	return 0
}
