package obrc

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
)

// WriteMeasurements writes the measurements to the provided file and prints progress after every 10% of lines
func WriteMeasurements(fileName string, size int) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("Failed to create file: %v", err)
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	defer bw.Flush()

	for i := 0; i < size; i++ {
		if i%100000 == 0 {
			fmt.Printf("Written %d lines\n",
				i)
		}
		nStations := len(Stations)
		station := Stations[rand.Intn(nStations)]
		line := station.ID + ";" + fmt.Sprintf("%.1f", station.Measurement()) + "\n"

		if _, err := bw.WriteString(line); err != nil {
			return fmt.Errorf("Failed to write line %d: %v", i, err)
		}
	}

	fmt.Println("Done writing file")
	return nil
}
