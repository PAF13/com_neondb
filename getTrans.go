package com_neondb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"time"

	n26 "github.com/PAF13/com_n26"
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
	query := `SELECT id, bookingdate, valuedate, partnername, partneriban, typess, paymentreference, accountname, amounteur, originalamount, originalcurrency, exchangerate FROM public.transactions_n26`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Query execution failed: %v\n", err)
	}
	defer rows.Close()

	// Slice to store results
	var records []n26.Records

	// Iterate through the result set.
	for rows.Next() {
		var record n26.Records
		err := rows.Scan(
			&record.ID,
			&record.BookingDate,
			&record.ValueDate,
			&record.PartnerName,
			&record.PartnerIBAN,
			&record.Type,
			&record.PaymentReference,
			&record.AccountName,
			&record.AmountEUR,
			&record.OriginalAmount,
			&record.OriginalCurrency,
			&record.ExchangeRate,
		)
		if err != nil {
			log.Fatalf("Row scan failed: %v\n", err)
		}
		records = append(records, record)
	}

	// Check for errors after iteration
	if err = rows.Err(); err != nil {
		log.Fatalf("Row iteration failed: %v\n", err)
	}
	fileJSON, _ := json.MarshalIndent(records, "", " ")

	_ = ioutil.WriteFile("temp/banktransactions.json", fileJSON, 0644)
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
			return *v
		} else {
			return ""
		}

	case *time.Time:
		return v.Format("2006-01-02")
	default:
		fmt.Println("Unsupported pointer type:", reflect.TypeOf(ptr))
		return ""
	}
}
