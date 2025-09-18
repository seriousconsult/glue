package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

// Define the paths to your input and output CSV files.
const (
	inputFilePath  = "large_file.csv"
	outputFilePath = "truncated_file.csv"
)

// Define which columns you want to keep (0-indexed).
var columnsToKeep = []int{0, 5, 10, 150}

func main() {
	// Use all available CPU cores for parallel processing.
	numWorkers := runtime.NumCPU()
	fmt.Printf("Starting CSV processing with %d workers...\n", numWorkers)

	// Set up channels to create a processing pipeline.
	rowsChan := make(chan []string, numWorkers)
	processedRowsChan := make(chan []string, numWorkers)

	var readerWg sync.WaitGroup
	var writerWg sync.WaitGroup

	// Goroutine 1: Read the input file and send rows to the pipeline.
	readerWg.Add(1)
	go readCSV(rowsChan, &readerWg)

	// Goroutines 2 to N: Worker pool to process rows concurrently.
	readerWg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go processRows(rowsChan, processedRowsChan, &readerWg)
	}

	// Goroutine N+1: Close the processedRowsChan once all workers are done.
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		readerWg.Wait() // Wait for all readers and workers to finish
		close(processedRowsChan)
	}()

	// Goroutine N+2: Write processed rows to the output file.
	writerWg.Add(1)
	go writeCSV(processedRowsChan, &writerWg)

	// Wait for the entire writing process to complete.
	writerWg.Wait()

	fmt.Println("CSV processing complete.")
}

// readCSV reads the input file and sends each row to the rowsChan.
func readCSV(rowsChan chan<- []string, wg *sync.WaitGroup) {
	defer close(rowsChan)
	defer wg.Done()

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("Error opening input file: %v", err)
	}
	defer inputFile.Close()

	reader := csv.NewReader(inputFile)
	reader.FieldsPerRecord = -1 // Allow malformed rows

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading row: %v. Skipping...", err)
			continue
		}
		rowsChan <- record
	}
}

// processRows reads from rowsChan, truncates the row, and sends it to processedRowsChan.
func processRows(rowsChan <-chan []string, processedRowsChan chan<- []string, wg *sync.WaitGroup) {
	defer wg.Done()

	for record := range rowsChan {
		newRecord := make([]string, 0, len(columnsToKeep))
		for _, colIndex := range columnsToKeep {
			if colIndex < len(record) {
				newRecord = append(newRecord, record[colIndex])
			} else {
				newRecord = append(newRecord, "")
			}
		}
		processedRowsChan <- newRecord
	}
}

// writeCSV reads from processedRowsChan and writes the rows to the output file.
func writeCSV(processedRowsChan <-chan []string, wg *sync.WaitGroup) {
	defer wg.Done()

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Error creating output file: %v", err)
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	for record := range processedRowsChan {
		if err := writer.Write(record); err != nil {
			log.Fatalf("Error writing record to output file: %v", err)
		}
	}
}

