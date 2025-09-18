package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
)

// Define the paths to your input and output CSV files.
const (
	inputFilePath  = "large_file.csv"
	outputFilePath = "combined_file.csv"
)

// Define the two column names to combine and the new column name.
const (
	column1Name       = "first_name"
	column2Name       = "last_name"
	combinedColumnName = "full_name"
)

func main() {
	// Use all available CPU cores for parallel processing.
	numWorkers := runtime.NumCPU()
	fmt.Printf("Starting CSV processing with %d workers...\n", numWorkers)

	// Open the input file and read the header to find the column indices.
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("Error opening input file: %v", err)
	}
	defer inputFile.Close()

	reader := csv.NewReader(inputFile)
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			log.Fatalf("Input file is empty or malformed: missing header.")
		}
		log.Fatalf("Error reading header from input file: %v", err)
	}

	// Find the column indices for the columns to combine.
	col1Index, col2Index := -1, -1
	for i, col := range header {
		if strings.TrimSpace(strings.ToLower(col)) == column1Name {
			col1Index = i
		}
		if strings.TrimSpace(strings.ToLower(col)) == column2Name {
			col2Index = i
		}
	}

	if col1Index == -1 || col2Index == -1 {
		log.Fatalf("Could not find both '%s' and '%s' columns in the CSV header.", column1Name, column2Name)
	}

	// Create the new header for the output file.
	newHeader := make([]string, len(header), len(header)+1)
	copy(newHeader, header)
	newHeader = append(newHeader, combinedColumnName)

	// Set up channels to create a processing pipeline.
	rowsChan := make(chan []string, numWorkers)
	processedRowsChan := make(chan []string, numWorkers)

	var readerWg sync.WaitGroup
	var writerWg sync.WaitGroup

	// Goroutine 1: Read the input file and send rows to the pipeline.
	readerWg.Add(1)
	go readCSV(inputFile, rowsChan, &readerWg)

	// Goroutines 2 to N: Worker pool to process and combine rows concurrently.
	readerWg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go processAndCombineRows(rowsChan, processedRowsChan, col1Index, col2Index, &readerWg)
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
	go writeCSV(newHeader, processedRowsChan, &writerWg)

	// Wait for the entire writing process to complete.
	writerWg.Wait()

	fmt.Println("CSV processing complete.")
}

// readCSV reads the input file (from the current position) and sends each row to the rowsChan.
func readCSV(inputFile *os.File, rowsChan chan<- []string, wg *sync.WaitGroup) {
	defer close(rowsChan)
	defer wg.Done()

	reader := csv.NewReader(inputFile)
	reader.FieldsPerRecord = -1 // Allow malformed rows

	// Skip the header since we already read it in main.
	if _, err := reader.Read(); err != nil && err != io.EOF {
		log.Printf("Error skipping header: %v", err)
	}

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

// processAndCombineRows reads from rowsChan, combines the specified columns, and sends it to processedRowsChan.
func processAndCombineRows(rowsChan <-chan []string, processedRowsChan chan<- []string, col1Index, col2Index int, wg *sync.WaitGroup) {
	defer wg.Done()

	for record := range rowsChan {
		// Create the new combined record.
		newRecord := make([]string, len(record), len(record)+1)
		copy(newRecord, record)
		
		// Combine the two column values.
		combinedValue := strings.TrimSpace(record[col1Index]) + " " + strings.TrimSpace(record[col2Index])
		newRecord = append(newRecord, combinedValue)
		
		processedRowsChan <- newRecord
	}
}

// writeCSV writes the header and processed rows to the output file.
func writeCSV(header []string, processedRowsChan <-chan []string, wg *sync.WaitGroup) {
	defer wg.Done()

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Error creating output file: %v", err)
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// Write the new header first.
	if err := writer.Write(header); err != nil {
		log.Fatalf("Error writing header to output file: %v", err)
	}

	for record := range processedRowsChan {
		if err := writer.Write(record); err != nil {
			log.Fatalf("Error writing record to output file: %v", err)
		}
	}
}

