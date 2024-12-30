package com_neondb

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

type Record struct {
	ID   int
	Name string
}

// test
func GetTrans() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	dsn := os.Getenv("DATABASE_URL")

	// Database connection configuration
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close(context.Background())

	// Query to fetch all records
	query := `SELECT id, name FROM public.transactions_n26`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Query execution failed: %v\n", err)
	}
	defer rows.Close()

	// Slice to store results
	var records []Record

	// Iterate through the result set
	for rows.Next() {
		var record Record
		err := rows.Scan(&record.ID, &record.Name)
		if err != nil {
			log.Fatalf("Row scan failed: %v\n", err)
		}
		records = append(records, record)
	}

	// Check for errors after iteration
	if err = rows.Err(); err != nil {
		log.Fatalf("Row iteration failed: %v\n", err)
	}

	// Print the fetched records
	for _, record := range records {
		fmt.Printf("ID: %d, Name: %s\n", record.ID, record.Name)
	}
}
