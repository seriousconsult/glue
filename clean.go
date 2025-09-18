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
var columnsToKeep = []int{0, 2, 12, 55, 57}

func main() {
	// Add line number and file name to log messages for better debugging.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)
	
	// Use all available CPU cores for parallel processing.
	numWorkers := runtime.NumCPU()
	fmt.Printf("Starting CSV processing with %d workers...\n", numWorkers)

	// Set up channels to create a processing pipeline.
	rowsChan := make(chan []string, numWorkers)
	processedRowsChan := make(chan []string, numWorkers)

	var readerWg sync.WaitGroup
	var writerWg sync.WaitGroup
	
	// Add a counter for rows to see how many are processed.
	var processedCount int
	var countMutex sync.Mutex

	// Goroutine 1: Read the input file and send rows to the pipeline.
	readerWg.Add(1)
	go readCSV(rowsChan, &readerWg)

	// Goroutines 2 to N: Worker pool to process rows concurrently.
	readerWg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go processRows(rowsChan, processedRowsChan, &readerWg, &countMutex, &processedCount)
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

	fmt.Printf("CSV processing complete. Total rows processed: %d\n", processedCount)
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
	// Allows rows with a variable number of fields.
	reader.FieldsPerRecord = -1

	lineCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			log.Printf("Read loop finished. Reached end of file at line %d.", lineCount)
			break
		}
		if err != nil {
			// Log a specific error about the read failure.
			log.Printf("Error reading row at line %d: %v. Skipping...", lineCount, err)
			continue // Skip the bad row and try the next one
		}
		
		// Log every 10,000 lines to track progress
		lineCount++
		if lineCount%10000 == 0 {
			log.Printf("Successfully read %d lines...", lineCount)
		}
		
		rowsChan <- record
	}
}

// processRows reads from rowsChan, truncates the row, and sends it to processedRowsChan.
func processRows(rowsChan <-chan []string, processedRowsChan chan<- []string, wg *sync.WaitGroup, countMutex *sync.Mutex, processedCount *int) {
	defer wg.Done()

	for record := range rowsChan {
		newRecord := make([]string, 0, len(columnsToKeep))
		for _, colIndex := range columnsToKeep {
			if colIndex < len(record) {
				newRecord = append(newRecord, record[colIndex])
			} else {
				// Handle malformed rows by adding a nil value
				newRecord = append(newRecord, "NULL")
				log.Printf("Warning: Column index %d is out of bounds for row with %d fields. Adding NULL.", colIndex, len(record))
			}
		}
		processedRowsChan <- newRecord
		
		// Safely increment the row counter
		countMutex.Lock()
		*processedCount++
		countMutex.Unlock()
	}
}

// writeCSV reads from processedRowsChan and writes the rows to the output file.
func writeCSV(processedRowsChan <-chan []string, wg *sync.WaitGroup) {
	defer wg.Done()

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Printf("Error creating output file: %v", err)
		return
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	for record := range processedRowsChan {
		if err := writer.Write(record); err != nil {
			log.Printf("Error writing record to output file: %v", err)
			continue
		}
	}
}
