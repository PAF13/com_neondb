package com_neondb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	n26 "github.com/PAF13/com_n26"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func valueReplace(object []*n26.Transaction) *[]string {
	file, err := ioutil.ReadFile(`queries\INSERT_BankTransfer.sql`)
	if err != nil {
		log.Fatal(err)
	}

	batch := []string{}
	length := len(object)
	for i, v := range object {
		if i != 0 {
			fmt.Println("Commands | adding " + strconv.Itoa(i) + " / " + strconv.Itoa(length-1))
			string_b := strings.Replace(string(file), "\r\n", "", -1)

			insertSQL := string(string_b)
			insertSQL = strings.Replace(insertSQL, "$1", checkNil(v.BookingDate), 1)

			insertSQL = strings.Replace(insertSQL, "$2", checkNil(v.ValueDate), 1)
			insertSQL = strings.Replace(insertSQL, "$3", checkNil(v.PartnerName), 1)
			insertSQL = strings.Replace(insertSQL, "$4", checkNil(v.PartnerIBAN), 1)
			insertSQL = strings.Replace(insertSQL, "$5", checkNil(v.Type), 1)
			insertSQL = strings.Replace(insertSQL, "$6", checkNil(v.PaymentReference), 1)
			insertSQL = strings.Replace(insertSQL, "$7", checkNil(v.AccountName), 1)
			insertSQL = strings.Replace(insertSQL, "$8", fmt.Sprintf("%f", v.AmountEUR), 1)
			insertSQL = strings.Replace(insertSQL, "$9", fmt.Sprintf("%f", v.OriginalAmount), 1)
			insertSQL = strings.Replace(insertSQL, "$10", checkNil(v.OriginalCurrency), 1)
			insertSQL = strings.Replace(insertSQL, "$11", fmt.Sprintf("%f", v.ExchangeRate), 1)

			batch = append(batch, insertSQL)
		}
	}

	fileJSON, _ := json.MarshalIndent(batch, "", " ")

	_ = ioutil.WriteFile("test.json", fileJSON, 0644)
	return &batch
}

func checkNil(value string) string {
	if value == "" {
		return "null"
	} else {
		value = strings.Replace(value, "'", "''", -1)
		return "'" + value + "'"
	}
}

func splitIntoChunks(slice *[]string, chunkSize int) *[][]string {
	var chunks [][]string
	for i := 0; i < len(*slice); i += chunkSize {
		end := i + chunkSize
		if end > len(*slice) {
			end = len(*slice)
		}
		chunks = append(chunks, (*slice)[i:end])
	}
	return &chunks
}

func createPool() (*pgxpool.Pool, error) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	dsn := os.Getenv("DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %v", err)
	}

	return pool, nil
}

func batchPool(pool *pgxpool.Pool, chunks *[][]string) {
	ctx := context.Background()

	// Acquire a connection from the pool
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Unable to acquire connection: %v", err)
	}
	defer conn.Release()

	batches := []*pgx.Batch{}

	for _, chunk := range *chunks {
		batches = append(batches, newBatch(&chunk))
	}

	// Execute each batch
	for i, batch := range batches {
		log.Printf("Executing batch %d", i+1)
		br := conn.SendBatch(ctx, batch)

		// Process results
		for j := 0; j < batch.Len(); j++ {
			ct, err := br.Exec()
			if err != nil {
				log.Fatalf("Batch execution failed: %v\n%v", err, *batch.QueuedQueries[j])
			}
			fmt.Printf("Batch %d - Rows affected: %v\n", i+1, ct.RowsAffected())
		}

		// Close the batch results
		if err := br.Close(); err != nil {
			log.Fatalf("Failed to close batch results: %v", err)
		}
	}
}
func newBatch(chunk *[]string) *pgx.Batch {
	batch := &pgx.Batch{}
	for _, b := range *chunk {
		batch.Queue(b)
	}

	return batch
}
func N26Upload(object []*n26.Transaction) {
	sqlCommands := valueReplace(object)

	// Split the slice into chunks of size 50
	chunkSize := 500
	chunks := splitIntoChunks(sqlCommands, chunkSize)

	// Print the results
	for i, chunk := range *chunks {
		fmt.Printf("Chunk %50d created | Size: %d\n", i+1, len(chunk))
	}
	pool, _ := createPool()
	batchPool(pool, chunks)
}
