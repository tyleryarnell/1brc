package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"time"

	obrc "github.com/tyleryarnell/1brc"
	"github.com/tyleryarnell/1brc/internal/baseline"
	one "github.com/tyleryarnell/1brc/internal/r01"
	two "github.com/tyleryarnell/1brc/internal/r02"
	three "github.com/tyleryarnell/1brc/internal/r03"
	four "github.com/tyleryarnell/1brc/internal/r04"
	five "github.com/tyleryarnell/1brc/internal/r05"
	six "github.com/tyleryarnell/1brc/internal/r06"
	seven "github.com/tyleryarnell/1brc/internal/r07"
	eight "github.com/tyleryarnell/1brc/internal/r08"
)

const usageMessage = `
Usage: %[1]s <command> [options]

Commands:
  create  Create measurements and save them to a file.
  run     Run the calculation using either the default (baseline) or a custom implementation.
  graph   Generate a graph based on previous runs.

Examples:
  Create Measurements:
    %[1]s create -size=5000000 -file="output.txt"
  Run Baseline Calculation:
    %[1]s run -version=0 -file="output.txt" -tracefile="trace.out" -cpuprofile="cpu.prof" -save-results -save-metrics -validate="expected.txt"
  Generate Graph:
    %[1]s graph
`

func main() {
	if len(os.Args) < 2 {
		fmt.Printf(usageMessage, os.Args[0])
		os.Exit(1)
	}

	// Parse common flags
	fileName := flag.String("file", "measurements.txt", "File name to read measurements")
	version := flag.Int("version", 0, "Version of the calculation to use")
	traceFile := flag.String("tracefile", "", "Enable execution tracing and save to the specified file")
	cpuProfileFile := flag.String("cpuprofile", "", "Enable CPU profiling and save to the specified file")
	saveResults := flag.Bool("save-results", false, "Save calculation results to a file")
	saveMetrics := flag.Bool("save-metrics", false, "Save time metrics to a file")
	validateFile := flag.String("validate", "", "Validate calculation results against the specified file")
	size := flag.Int("size", 10000000, "Number of records to create")

	// First, we need to check the command, which comes before the flags.
	command := os.Args[1]

	// Parse the flags
	flag.CommandLine.Parse(os.Args[2:])

	switch command {
	case "create":
		createMeasurements(*size, *fileName)
	case "run":
		handleRunCommand(*fileName, *version, *traceFile, *cpuProfileFile, *saveResults, *saveMetrics, *validateFile)
	case "graph":
		handleGraphCommand()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Printf(usageMessage, os.Args[0])
		os.Exit(1)
	}
}

// handleRunCommand processes the "run" command with optional tracing, CPU profiling, conditional result saving, and validation.
func handleRunCommand(fileName string, version int, traceFile string, cpuProfileFile string, saveResults bool, saveMetrics bool, validateFile string) {
	// Start tracing if specified
	if traceFile != "" {
		f, err := os.Create(traceFile)
		if err != nil {
			fmt.Printf("Could not create trace file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		if err := trace.Start(f); err != nil {
			fmt.Printf("Could not start trace: %v\n", err)
			os.Exit(1)
		}
		defer trace.Stop()
	}

	// Start CPU profiling if specified
	if cpuProfileFile != "" {
		f, err := os.Create(cpuProfileFile)
		if err != nil {
			fmt.Printf("Could not create CPU profile file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("Could not start CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	runCalculation(fileName, version, saveResults, saveMetrics, validateFile)
}

// handleGraphCommand processes the "graph" command.
func handleGraphCommand() {
	obrc.GraphResults()
}

// createMeasurements generates a set of measurements and saves them to a file.
func createMeasurements(size int, fileName string) {
	fmt.Printf("Creating %d measurements...\n", size)

	// Write the measurements to the specified file
	if err := obrc.WriteMeasurements(fileName, size); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Measurements created successfully.")
}

// runCalculation performs the calculation, conditionally saving results, saving time metrics, and optionally validating the output.
func runCalculation(fileName string, version int, saveResults bool, saveMetrics bool, validateFile string) {
	var calculator obrc.Calculator

	switch version {
	case 1:
		calculator = obrc.CalculateFunc(one.Calculate)
		fmt.Println("Using iterator implementation...")
	case 2:
		calculator = obrc.CalculateFunc(two.Calculate)
		fmt.Println("Using buffered reader implementation...")
	case 3:
		calculator = obrc.CalculateFunc(three.Calculate)
		fmt.Println("Using map assigns implementation...")
	case 4:
		calculator = obrc.CalculateFunc(four.Calculate)
		fmt.Println("Using parse as bytes implementation...")
	case 5:
		calculator = obrc.CalculateFunc(five.Calculate)
		fmt.Println("Using improved bytes parsing implementation...")
	case 6:
		calculator = obrc.CalculateFunc(six.Calculate)
		fmt.Println("Using byte parsing and int conversion implementation...")
	case 7:
		calculator = obrc.CalculateFunc(seven.Calculate)
		fmt.Println("Using custom hash table implementation...")
	case 8:
		calculator = obrc.CalculateFunc(eight.Calculate)
		fmt.Println("Using parallel file chunking implementation...")
	default:
		calculator = obrc.CalculateFunc(baseline.Calculate)
		fmt.Println("Using the default (baseline) implementation...")
	}

	// Get measurements size for directory creation, e.g., "measurements.1b.txt" -> "1b"
	dataSize := strings.Split(fileName, ".")[1]

	// Create the base runs directory if it doesn't exist
	if err := os.MkdirAll("runs", 0755); err != nil {
		fmt.Printf("Failed to create runs directory: %v\n", err)
		return
	}

	// Generate a timestamp and create a directory for this run under the dataSize directory
	timestamp := time.Now().Format("20060102_150405")
	runDir := filepath.Join("runs", dataSize, timestamp)

	if err := os.MkdirAll(runDir, 0755); err != nil {
		fmt.Printf("Failed to create run directory: %v\n", err)
		return
	}

	var outputFile *os.File
	if saveResults {
		// Create output file in the run directory
		outputFileName := filepath.Join(runDir, "results.txt")
		var err error
		outputFile, err = os.Create(outputFileName)
		if err != nil {
			fmt.Printf("Failed to create output file: %v\n", err)
			return
		}
		defer outputFile.Close()
	} else {
		// Discard output if not saving results
		outputFile = os.Stdout
	}

	// Measure time taken and run the calculation
	start := time.Now()
	if err := calculator.Calculate(fileName, outputFile); err != nil {
		fmt.Printf("Error running calculation: %v\n", err)
		return
	}
	duration := time.Since(start)
	fmt.Printf("Calculation completed in %v\n", duration)

	// Save the time metrics if requested
	if saveMetrics {
		saveTimeMetrics(duration, runDir)
	}

	// Validate output if validation file is specified
	if validateFile != "" {
		fmt.Println("Validating results...")
		validateResults(validateFile, outputFile)
	}
}

// validateResults compares the results of the calculation with a saved results file.
func validateResults(validateFile string, outputFile *os.File) {
	expectedBytes, err := os.ReadFile(validateFile)
	if err != nil {
		fmt.Printf("Failed to read validation file: %v\n", err)
		return
	}

	fmt.Println("Reading output file for validation...")
	outputBytes, err := io.ReadAll(outputFile)
	if err != nil {
		fmt.Printf("Failed to read output file: %v\n", err)
		return
	}

	fmt.Println("Comparing output with expected results...")
	if string(expectedBytes) == string(outputBytes) {
		fmt.Println("Validation successful: output matches the expected results.")
	} else {
		fmt.Println("Validation failed: output does not match the expected results.")
	}
}

// saveTimeMetrics saves the time taken for calculation to a file if the flag is enabled.
func saveTimeMetrics(duration time.Duration, runDir string) {
	metricsFileName := filepath.Join(runDir, "time_metrics.txt")
	metricsFile, err := os.Create(metricsFileName)
	if err != nil {
		fmt.Printf("Failed to create metrics file: %v\n", err)
		return
	}
	defer metricsFile.Close()

	if _, err := metricsFile.WriteString(fmt.Sprintf("Time taken: %v ms\n", duration.Milliseconds())); err != nil {
		fmt.Printf("Failed to write time metrics to file: %v\n", err)
	}
	fmt.Printf("Time metrics saved to '%s'.\n", metricsFileName)
}
