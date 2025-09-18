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

// readPhoneNumbersFromFile reads a CSV file, finds the specified column,
// and returns a map of unique phone numbers. It uses a buffered reader for better I/O performance.
func readPhoneNumbersFromFile(filepath string, columnName string) (map[string]struct{}, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("could not open file %s: %w", filepath, err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("could not read header from file %s: %w", filepath, err)
	}
	
	colIndex := -1
	for i, col := range header {
		if strings.EqualFold(strings.TrimSpace(col), strings.TrimSpace(columnName)) {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return nil, fmt.Errorf("column '%s' not found in file %s", columnName, filepath)
	}

	phoneSet := make(map[string]struct{})
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Warning: Skipping malformed row in %s: %v", filepath, err)
			continue
		}
		
		if colIndex < len(record) {
			phoneSet[record[colIndex]] = struct{}{}
		}
	}
	return phoneSet, nil
}

// countCommonPhoneNumbers streams through the second file concurrently, finds the specified
// column, and counts how many phone numbers also exist in the provided in-memory set.
// It uses a worker pool to parallelize the processing and prints a progress update.
func countCommonPhoneNumbers(filepath string, phoneSet map[string]struct{}, columnName string) (int32, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return 0, fmt.Errorf("could not open file %s: %w", filepath, err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	header, err := reader.Read()
	if err != nil {
		return 0, fmt.Errorf("could not read header from file %s: %w", filepath, err)
	}

	colIndex := -1
	for i, col := range header {
		if strings.EqualFold(strings.TrimSpace(col), strings.TrimSpace(columnName)) {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return 0, fmt.Errorf("column '%s' not found in file %s", columnName, filepath)
	}

	var commonCount int32
	var wg sync.WaitGroup
	rowsChan := make(chan []string, runtime.NumCPU())

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for record := range rowsChan {
				if colIndex < len(record) {
					if _, ok := phoneSet[record[colIndex]]; ok {
						atomic.AddInt32(&commonCount, 1)
					}
				}
			}
		}()
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			count := atomic.LoadInt32(&commonCount)
			log.Printf("Processing... Found %d common numbers so far.", count)
		}
	}()

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Warning: Skipping malformed row in %s: %v", filepath, err)
			continue
		}
		rowsChan <- record
	}
	
	close(rowsChan)
	wg.Wait()
	return commonCount, nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <file1.csv> <file2.csv>\n", os.Args[0])
		os.Exit(1)
	}

	file1Path := os.Args[1]
	file2Path := os.Args[2]
	
	file1Column := "dialednum"
	file2Column := "LINEA2"
	
	fmt.Printf("Step 1: Loading unique phone numbers from '%s' into memory using column '%s'...\n", file1Path, file1Column)
	phoneSet, err := readPhoneNumbersFromFile(file1Path, file1Column)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("Loaded %d unique phone numbers from '%s'.\n", len(phoneSet), file1Path)

	fmt.Printf("\nStep 2: Streaming through '%s' concurrently using column '%s' to find common phone numbers with %d workers...\n", file2Path, file2Column, runtime.NumCPU())
	commonCount, err := countCommonPhoneNumbers(file2Path, phoneSet, file2Column)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Printf("\nDone.\n")
	fmt.Printf("Total count of phone numbers found in both files: %d\n", commonCount)
}
