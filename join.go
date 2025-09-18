package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// readPhoneNumbersFromFile reads a CSV file, finds the "phone_number" column,
// and returns a map of unique phone numbers. It uses a buffered reader for better I/O performance.
func readPhoneNumbersFromFile(filepath string) (map[string]struct{}, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer file.Close()

	phoneNumbers := make(map[string]struct{})
	reader := csv.NewReader(bufio.NewReader(file))

	// Read the header to find the "phone_number" column index.
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return phoneNumbers, nil
		}
		return nil, fmt.Errorf("failed to read header from %s: %w", filepath, err)
	}

	phoneColumnIndex := -1
	for i, col := range header {
		if strings.TrimSpace(strings.ToLower(col)) == "phone_number" {
			phoneColumnIndex = i
			break
		}
	}
	if phoneColumnIndex == -1 {
		return nil, fmt.Errorf("could not find 'phone_number' column in file %s", filepath)
	}

	// For debugging, keep track of the first few unique numbers added.
	var debugAddedCount int
	const debugLimit = 5

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Warning: failed to read record from %s, skipping: %v\n", filepath, err)
			continue
		}

		if len(record) > phoneColumnIndex {
			phoneNumber := strings.TrimSpace(record[phoneColumnIndex])
			if phoneNumber != "" {
				if _, exists := phoneNumbers[phoneNumber]; !exists && debugAddedCount < debugLimit {
					fmt.Printf("DEBUG: Adding unique phone number from '%s': '%s'\n", filepath, phoneNumber)
					debugAddedCount++
				}
				phoneNumbers[phoneNumber] = struct{}{}
			}
		}
	}

	return phoneNumbers, nil
}

// countCommonPhoneNumbers streams through the second file concurrently, finds the "phone_number"
// column, and counts how many phone numbers also exist in the provided in-memory set.
// It uses a worker pool to parallelize the processing and prints a progress update.
func countCommonPhoneNumbers(filepath string, phoneSet map[string]struct{}) (int32, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	// Read the header to find the "phone_number" column index.
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read header from %s: %w", filepath, err)
	}

	phoneColumnIndex := -1
	for i, col := range header {
		if strings.TrimSpace(strings.ToLower(col)) == "phone_number" {
			phoneColumnIndex = i
			break
		}
	}
	if phoneColumnIndex == -1 {
		return 0, fmt.Errorf("could not find 'phone_number' column in file %s", filepath)
	}

	recordsChan := make(chan []string)
	var commonCount int32
	var linesRead int32
	var wg sync.WaitGroup
	var debugFoundCount int32
	const debugLimit int32 = 5

	// Determine the number of workers based on available CPU cores.
	numWorkers := runtime.NumCPU()
	if numWorkers == 0 {
		numWorkers = 1
	}

	// Add the number of workers to the WaitGroup.
	wg.Add(numWorkers)
	// Start worker goroutines.
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for record := range recordsChan {
				if len(record) > phoneColumnIndex {
					phoneNumber := strings.TrimSpace(record[phoneColumnIndex])
					if phoneNumber != "" {
						if _, exists := phoneSet[phoneNumber]; exists {
							if atomic.LoadInt32(&debugFoundCount) < debugLimit {
								fmt.Printf("DEBUG: Found common phone number in '%s': '%s'\n", filepath, phoneNumber)
								atomic.AddInt32(&debugFoundCount, 1)
							}
							atomic.AddInt32(&commonCount, 1)
						}
					}
				}
			}
		}()
	}

	// Start a goroutine to read records from the CSV file and send them to the channel.
	go func() {
		defer close(recordsChan)
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Warning: failed to read record from %s, skipping: %v\n", filepath, err)
				continue
			}
			recordsChan <- record
			atomic.AddInt32(&linesRead, 1)
		}
	}()

	// Goroutine to print progress.
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fmt.Printf("Processed %d lines. Current count of common phone numbers: %d\n", atomic.LoadInt32(&linesRead), atomic.LoadInt32(&commonCount))
			}
		}
	}()

	// Wait for all workers to finish.
	// This will only happen after the reader goroutine closes the channel.
	wg.Wait()

	// Stop the ticker and the progress goroutine.
	ticker.Stop()
	done <- true

	return commonCount, nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <file1.csv> <file2.csv>\n", os.Args[0])
		os.Exit(1)
	}

	file1Path := os.Args[1]
	file2Path := os.Args[2]

	fmt.Printf("Step 1: Loading unique phone numbers from '%s' into memory...\n", file1Path)
	phoneSet, err := readPhoneNumbersFromFile(file1Path)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("Loaded %d unique phone numbers from '%s'.\n", len(phoneSet), file1Path)

	fmt.Printf("\nStep 2: Streaming through '%s' concurrently to find common phone numbers with %d workers...\n", file2Path, runtime.NumCPU())
	commonCount, err := countCommonPhoneNumbers(file2Path, phoneSet)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Printf("\nDone.\n")
	fmt.Printf("Total count of phone numbers found in both files: %d\n", commonCount)
}

