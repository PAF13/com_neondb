package com_neondb

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

// added date
type Record struct {
	bookingdate *time.Time
	partnername *string
	partneriban *string
	typess      *string
	accountname *string
	amounteur   *float32
}

// test2
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
	query := `SELECT bookingdate, partnername, partneriban, typess, accountname, amounteur FROM public.transactions_n26`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Query execution failed: %v\n", err)
	}
	defer rows.Close()

	// Slice to store results
	var records []Record

	// Iterate through the result set.
	for rows.Next() {
		var record Record
		err := rows.Scan(
			&record.bookingdate,
			&record.partnername,
			&record.partneriban,
			&record.typess,
			&record.accountname,
			&record.amounteur)
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
		amount := fmt.Sprintf("%f", *record.amounteur)
		fmt.Println(
			processPointer(*record.bookingdate) +
				processPointer(*record.partnername) +
				processPointer(*record.partneriban) +
				processPointer(*record.typess) +
				processPointer(*record.accountname) +
				amount)
	}
}

func processPointer(ptr interface{}) string {
	// Check if the input is a pointer using reflection
	if reflect.TypeOf(ptr).Kind() != reflect.Ptr {
		fmt.Println("Expected a pointer, but received:", reflect.TypeOf(ptr).Kind())
		return ""
	}

	// Dereference the pointer to get its value
	switch v := ptr.(type) {
	case *string:
		if v != nil {
			return ""
		} else {
			return *v
		}
	case *time.Time:
		if v != nil {
			return ""
		} else {
			return v.Format("YYYY-MM-DD")
		}
	default:
		fmt.Println("Unsupported pointer type:", reflect.TypeOf(ptr))
		return ""
	}
	return ""
}
