package obrc

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// GraphResults generates line graphs of the benchmark results over time for each data size.
func GraphResults() {
	// Base directory for the runs
	runsDir := "runs"

	// Get all data size directories (e.g., "1b", "10mb")
	dataSizeDirs, err := os.ReadDir(runsDir)
	if err != nil {
		fmt.Printf("Failed to read 'runs' directory: %v\n", err)
		return
	}

	// Iterate over each data size directory
	for _, dataSizeDir := range dataSizeDirs {
		if dataSizeDir.IsDir() {
			dataSize := dataSizeDir.Name()

			// Collect data from the data size directory
			dataSizePath := filepath.Join(runsDir, dataSize)
			runDirs, err := os.ReadDir(dataSizePath)
			if err != nil {
				fmt.Printf("Failed to read directory '%s': %v\n", dataSizePath, err)
				continue
			}

			var points plotter.XYs
			for _, runDir := range runDirs {
				if runDir.IsDir() {
					metricsFilePath := filepath.Join(dataSizePath, runDir.Name(), "time_metrics.txt")
					if _, err := os.Stat(metricsFilePath); err == nil {
						content, err := os.ReadFile(metricsFilePath)
						if err != nil {
							fmt.Printf("Failed to read file '%s': %v\n", metricsFilePath, err)
							continue
						}
						lines := strings.Split(string(content), "\n")
						for _, line := range lines {
							if strings.HasPrefix(line, "Time taken: ") {
								timeTakenStr := strings.TrimPrefix(line, "Time taken: ")
								timeTaken, err := strconv.ParseFloat(strings.TrimSuffix(timeTakenStr, " ms"), 64)
								if err == nil {
									points = append(points, plotter.XY{X: float64(len(points) + 1), Y: timeTaken})
								}
							}
						}
					}
				}
			}

			// If there are points to plot, create the graph
			if len(points) > 0 {
				p := plot.New()

				p.Title.Text = fmt.Sprintf("Benchmark Results Over Time - %s", dataSize)
				p.X.Label.Text = "Run"
				p.Y.Label.Text = "Time Taken (ms)"

				// Create a line plot
				line, err := plotter.NewLine(points)
				if err != nil {
					fmt.Printf("Failed to create line plot: %v\n", err)
					continue
				}

				p.Add(line)

				// Save the plot to a PNG file within the data size directory
				graphFileName := filepath.Join(dataSizePath, fmt.Sprintf("benchmark_results_%s.png", dataSize))
				if err := p.Save(6*vg.Inch, 4*vg.Inch, graphFileName); err != nil {
					fmt.Printf("Failed to save plot: %v\n", err)
					continue
				}

				fmt.Printf("Graph saved to '%s'.\n", graphFileName)
			} else {
				fmt.Printf("No data to plot for data size '%s'.\n", dataSize)
			}
		}
	}
}
