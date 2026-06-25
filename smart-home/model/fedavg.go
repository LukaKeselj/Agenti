package model

// FedAvg aggregates client models using weighted averaging.
//   w_global = Σ(n_k / N) × w_k
//
// clientWeights is a slice of flat weight vectors (one per client).
// numSamples is the number of training samples each client used.
func FedAvg(clientWeights [][]float64, numSamples []int) []float64 {
	if len(clientWeights) == 0 {
		return nil
	}
	if len(clientWeights) != len(numSamples) {
		return nil
	}

	totalSamples := 0
	for _, n := range numSamples {
		totalSamples += n
	}
	if totalSamples == 0 {
		return nil
	}

	nClients := len(clientWeights)
	nWeights := len(clientWeights[0])
	aggregated := make([]float64, nWeights)

	for c := 0; c < nClients; c++ {
		ratio := float64(numSamples[c]) / float64(totalSamples)
		for i := 0; i < nWeights; i++ {
			aggregated[i] += clientWeights[c][i] * ratio
		}
	}

	return aggregated
}
