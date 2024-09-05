package one

import (
	"bufio"
	"fmt"
	"io"
	"iter"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Calculate reads the input and calculates the min, average, and max values for each station
func Calculate(inputFile string, output io.Writer) error {

	// Open the file to be processed
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	type stats struct {
		min, max, sum float64
		count         int
	}

	measurements := make(map[string]stats)
	cnt := 0
	for line := range getMeasurements(file) {
		parts := strings.Split(line, ";")
		if len(parts) != 2 {
			panic(fmt.Sprintf("Malformed line: %q", line))
		}

		station := parts[0]
		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			panic(fmt.Sprintf("Failed to parse value %q: %v", parts[1], err))
		}

		cnt++
		if _, exists := measurements[station]; !exists {
			measurements[station] = stats{min: value, max: value, sum: value, count: 1}
			continue
		}

		m := measurements[station]
		m.min = min(m.min, value)
		m.max = max(m.max, value)
		m.sum += value
		m.count++

		measurements[station] = m
	}

	fmt.Println("cnt", cnt)
	// Sort the station names
	sortedKeys := make([]string, 0, len(measurements))
	for key := range measurements {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	// Write results to the output writer
	var builder strings.Builder
	builder.WriteString("{")
	for i, station := range sortedKeys {
		res := fmt.Sprintf("%s=%.1f/%.1f/%.1f", station,
			measurements[station].min,
			measurements[station].sum/float64(measurements[station].count),
			measurements[station].max)
		builder.WriteString(res)
		if i < len(sortedKeys)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString("}\n")
	if _, err := output.Write([]byte(builder.String())); err != nil {
		return fmt.Errorf("Failed to write results to output: %v", err)
	}

	return nil
}

func getMeasurements(inp io.Reader) iter.Seq[string] {
	scanner := bufio.NewScanner(inp)
	return func(yield func(string) bool) {
		for scanner.Scan() {
			if !yield(scanner.Text()) {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			// Handle the error as needed
			return
		}
	}
}
