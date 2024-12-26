package com_neondb

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	n26 "github.com/PAF13/com_n26"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

// prep a map to feed the batch sql
func N26Upload(object []*n26.Transaction) {
	n26BatchSend(valueReplace(object))
}
func valueReplace(object []*n26.Transaction) *[]string {
	file, err := ioutil.ReadFile(`queries\INSERT_BankTransfer.sql`)
	if err != nil {
		log.Fatal(err)
	}

	batch := []string{}
	length := len(object)
	for i, v := range object {
		fmt.Println("adding " + string(i) + " / " + string(length))
		insertSQL := string(file)
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

	return &batch
}

func checkNil(value string) string {
	if value == "" {
		return "null"
	} else {
		return "`" + value + "`"
	}
}
func n26BatchSend(commands *[]string) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	connStr := os.Getenv("DATABASE_URL")
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Create a batch
	batch := &pgx.Batch{}

	for _, b := range *commands {
		batch.Queue(b)
	}
	// Send the batch
	br := conn.SendBatch(ctx, batch)
	defer br.Close()

	// Handle batch results
	for i := 0; i < len(*commands); i++ {
		_, err := br.Exec()
		if err != nil {
			log.Printf("Error executing query %d: %v", i+1, err)
		} else {
			log.Printf("Query %d executed successfully.", i+1)
		}
	}
}