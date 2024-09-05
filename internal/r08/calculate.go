package eight

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// Calculate reads the input and calculates the min, average, and max values for each station
func Calculate(inputFile string, output io.Writer) error {

	// Open the file to be processed
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Memory map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to memory-map the file: %v", err)
	}
	defer syscall.Munmap(data)

	// Determine the number of available CPU cores
	numCores := runtime.GOMAXPROCS(0)

	chunkSize := fileSize / 100

	wg := sync.WaitGroup{}
	resultChan := make(chan result, numCores)

	// Initialize start at the beginning of the file
	start := int64(0)

	// Process each chunk in parallel, ensuring continuous chunking
	for i := 0; i < numCores; i++ {
		// Calculate the end of the current chunk
		end := start + chunkSize

		// Adjust the end to the next newline to ensure we end at a line boundary
		if end < fileSize {
			for end < fileSize && data[end] != '\n' {
				end++
			}
			end++ // Move to the first character after the newline
		}

		// Increment the wait group
		wg.Add(1)

		// Process the chunk in a goroutine
		go func(start, end int64) {
			defer wg.Done()

			// Create a reader for the chunk and process it
			chunk := data[start:end]
			processChunk(bytes.NewReader(chunk), resultChan)

		}(start, end)

		// Move start to the end of this chunk for the next iteration
		start = end

		// If we've processed the whole file, break the loop
		if start >= fileSize {
			break
		}
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	measurements := make(map[string]*stats)
	// Merge the results
	for res := range resultChan {
		station, stats := string(res.station), res.stats
		s := measurements[station]
		if s == nil {
			measurements[station] = stats
		} else {
			s.min = min(s.min, stats.min)
			s.max = max(s.max, stats.max)
			s.sum += stats.sum
			s.count += stats.count
		}
	}

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
			float64(measurements[station].min/10),
			float64(measurements[station].sum)/float64(measurements[station].count)/10,
			float64(measurements[station].max)/10)
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

type result struct {
	station []byte
	stats   *stats
}

func processChunk(inp io.Reader, out chan<- result) {
	hashTable := newHashTable()

	for line := range getMeasurements(inp) {
		station, value := parseRow(line)

		// Insert or update hash table
		hashTable.insertOrUpdate(station, value)
	}

	for station, stats := range hashTable.All() {
		out <- result{station, stats}
	}
}

// parse row backwards
func parseRow(row []byte) ([]byte, int32) {
	// Find the last comma in the row

	// parse backwards according to
	// Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
	nRow := len(row) - 1 // last index
	temp := int32(row[nRow] - '0')

	nRow -= 2 // skip the last digit and the dot
	temp = temp + int32(row[nRow]-'0')*10

	nRow--

	if row[nRow] >= '0' && row[nRow] <= '9' {
		temp = temp + int32(row[nRow]-'0')*100
		nRow--
	}

	if row[nRow] == '-' {
		temp = -temp
		nRow--
	}

	nRow-- // skip the semicolon

	return row[:nRow+1], temp
}

func getMeasurements(inp io.Reader) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		var (
			buf  = make([]byte, 1024*1024) // 1MB buffer
			next int                       // Next start position in the buffer
		)

		// Read the file in chunks
		for {
			// Read into the buffer starting from 'next'
			n, err := inp.Read(buf[next:])
			if err != nil && err != io.EOF {
				return
			}
			if n == 0 && next == 0 {
				break
			}

			// Only process the valid portion of the buffer
			validBuf := buf[:next+n]

			// Find the last newline in the buffer
			lastNewLineIdx := bytes.LastIndexByte(validBuf, '\n')

			if lastNewLineIdx == -1 {
				// No newline found, continue reading more data
				next = len(validBuf)
				if next == cap(buf) {
					// If the buffer is full without any newlines, there's an error
					panic("line exceeds buffer size")
				}
				continue
			}

			// Process each line in the chunk up to the last newline
			chunk := validBuf[:lastNewLineIdx+1]
			remainder := validBuf[lastNewLineIdx+1:]

			for {
				nextLine := bytes.IndexByte(chunk, '\n')
				if nextLine == -1 {
					break
				}
				line := chunk[:nextLine]
				chunk = chunk[nextLine+1:]
				if !yield(line) {
					return
				}
			}

			// Copy the remainder to the start of the buffer for the next read
			next = copy(buf, remainder)

			// If we reached EOF, break out of the loop
			if err == io.EOF {
				break
			}
		}
	}
}
