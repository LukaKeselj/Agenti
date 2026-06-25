package evaluation

import (
	"fmt"
	"math"
	"strings"
)

// Metrics holds standard regression evaluation metrics.
type Metrics struct {
	MSE  float64
	RMSE float64
	MAE  float64
	R2   float64
}

// Calculate computes MSE, RMSE, MAE, and R² given actual and predicted values.
func Calculate(actual, predicted []float64) Metrics {
	n := len(actual)
	if n == 0 || n != len(predicted) {
		return Metrics{}
	}

	var mse, mae float64
	meanActual := 0.0
	for _, v := range actual {
		meanActual += v
	}
	meanActual /= float64(n)

	var ssRes, ssTot float64
	for i := 0; i < n; i++ {
		diff := actual[i] - predicted[i]
		mse += diff * diff
		mae += math.Abs(diff)
		ssRes += diff * diff
		totDiff := actual[i] - meanActual
		ssTot += totDiff * totDiff
	}
	mse /= float64(n)
	mae /= float64(n)

	r2 := 1.0
	if ssTot > 1e-12 {
		r2 = 1.0 - ssRes/ssTot
	}

	return Metrics{
		MSE:  math.Round(mse*1e6) / 1e6,
		RMSE: math.Round(math.Sqrt(mse)*1e6) / 1e6,
		MAE:  math.Round(mae*1e6) / 1e6,
		R2:   math.Round(r2*1e6) / 1e6,
	}
}

// ── Convergence table ─────────────────────────────────────────

// RoundResult contains per-round evaluation data for the table.
type RoundResult struct {
	Round   int
	Metrics Metrics
}

// PrintConvergenceTable prints a formatted convergence table to stdout.
func PrintConvergenceTable(results []RoundResult) {
	if len(results) == 0 {
		fmt.Println("(no data)")
		return
	}

	header := fmt.Sprintf("%-6s | %-10s | %-10s | %-10s | %-10s",
		"Round", "MSE", "RMSE", "MAE", "R²")
	sep := strings.Repeat("-", len(header))

	fmt.Println("\n╔══════════════════════════════════════════════════╗")
	fmt.Println("║         Convergence Table (per round)           ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Println(header)
	fmt.Println(sep)

	for _, r := range results {
		fmt.Printf("%-6d | %-10.6f | %-10.6f | %-10.6f | %-10.6f\n",
			r.Round, r.Metrics.MSE, r.Metrics.RMSE, r.Metrics.MAE, r.Metrics.R2)
	}
	fmt.Println()
}

// PrintSummary prints a one-line summary of the final metrics.
func PrintSummary(rounds int, final Metrics) {
	fmt.Println("══════════════════════════════════════════")
	fmt.Println("Training complete after", rounds, "rounds")
	fmt.Println("══════════════════════════════════════════")
	fmt.Printf("  MSE:  %.6f\n", final.MSE)
	fmt.Printf("  RMSE: %.6f\n", final.RMSE)
	fmt.Printf("  MAE:  %.6f\n", final.MAE)
	fmt.Printf("  R²:   %.6f\n", final.R2)
	fmt.Println()
}
