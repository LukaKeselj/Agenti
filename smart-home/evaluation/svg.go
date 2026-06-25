package evaluation

import (
	"fmt"
	"math"
	"os"
)

func GenerateSVG(path string, results []RoundResult) error {
	if len(results) == 0 {
		return nil
	}

	width, height := 800, 400
	marginL, marginR, marginT, marginB := 60, 30, 30, 50
	plotW := width - marginL - marginR
	plotH := height - marginT - marginB

	var maxMSE, minMSE float64 = math.Inf(-1), math.Inf(1)
	var maxR2, minR2 float64 = math.Inf(-1), math.Inf(1)
	for _, r := range results {
		if r.Metrics.MSE > maxMSE {
			maxMSE = r.Metrics.MSE
		}
		if r.Metrics.MSE < minMSE {
			minMSE = r.Metrics.MSE
		}
		if r.Metrics.R2 > maxR2 {
			maxR2 = r.Metrics.R2
		}
		if r.Metrics.R2 < minR2 {
			minR2 = r.Metrics.R2
		}
	}
	if maxMSE == minMSE {
		maxMSE = minMSE + 0.1
	}

	n := len(results)
	x := func(i int) float64 {
		return float64(marginL) + float64(i)*float64(plotW)/float64(n-1)
	}
	yMSE := func(v float64) float64 {
		return float64(marginT+plotH) - (v-minMSE)/(maxMSE-minMSE)*float64(plotH)
	}
	yR2 := func(v float64) float64 {
		return float64(marginT+plotH) - (v-minR2)/(maxR2-minR2)*float64(plotH)
	}

	msePoints := ""
	r2Points := ""
	for i, r := range results {
		msePoints += fmt.Sprintf("%.1f,%.1f ", x(i), yMSE(r.Metrics.MSE))
		r2Points += fmt.Sprintf("%.1f,%.1f ", x(i), yR2(r.Metrics.R2))
	}

	gridLines := ""
	for i := 0; i <= 4; i++ {
		frac := float64(i) / 4.0
		gy := float64(marginT) + float64(plotH)*frac
		gridLines += fmt.Sprintf(
			`<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#eee" stroke-width="1"/>`,
			marginL, gy, width-marginR, gy)
	}

	xAxisLabels := ""
	step := n / 10
	if step < 1 {
		step = 1
	}
	for i := 0; i < n; i += step {
		xAxisLabels += fmt.Sprintf(
			`<text x="%.1f" y="%d" text-anchor="middle" font-size="12" fill="#666">%d</text>`,
			x(i), height-marginB+20, results[i].Round)
	}

	content := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d">
  <rect width="%d" height="%d" fill="#fff"/>
  <text x="%d" y="20" text-anchor="middle" font-size="16" font-weight="bold" fill="#333">Convergence — MSE and R² per FL Round</text>

  <!-- MSE axis label -->
  <text x="15" y="%d" text-anchor="middle" font-size="12" fill="#d32f2f" transform="rotate(-90,15,%d)">MSE</text>
  <!-- R² axis label -->
  <text x="%d" y="%d" text-anchor="middle" font-size="12" fill="#388e3c" transform="rotate(-90,%d,%d)">R²</text>

  <!-- grid -->
%s  
  <!-- axes -->
  <line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333" stroke-width="1"/>
  <line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333" stroke-width="1"/>

  <!-- x-axis labels -->
%s
  <!-- MSE line -->
  <polyline points="%s" fill="none" stroke="#d32f2f" stroke-width="2"/>
  <!-- R² line -->
  <polyline points="%s" fill="none" stroke="#388e3c" stroke-width="2" stroke-dasharray="5,3"/>

  <!-- legend -->
  <rect x="%d" y="%d" width="12" height="12" fill="#d32f2f"/>
  <text x="%d" y="%d" font-size="12" fill="#333">MSE</text>
  <line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#388e3c" stroke-width="2" stroke-dasharray="5,3"/>
  <text x="%d" y="%d" font-size="12" fill="#333">R²</text>
</svg>`,
		width, height, width, height, width/2,
		marginT/2+plotH/2, marginT/2+plotH/2,
		width-marginR+10, marginT/2+plotH/2, width-marginR+10, marginT/2+plotH/2,
		gridLines,
		marginL, marginT+plotH, width-marginR, marginT+plotH,
		marginL, marginT, marginL, marginT+plotH,
		xAxisLabels,
		msePoints, r2Points,
		width-140, marginT+10, width-125, marginT+20,
		width-140, marginT+40, width-125, marginT+50,
		width-140, marginT+50)

	return os.WriteFile(path, []byte(content), 0644)
}
