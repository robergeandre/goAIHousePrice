package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
	ffmt "gopkg.in/ffmt.v1"
)

func main() {

	ffmt.Puts("=== Start Program ===")
	f, err := os.Open("train.csv")
	mHandleErr(err)
	hdr, data, indices, err := ingest(f)

	mHandleErr(err)

	fmt.Printf("Original Data: \nRows: %d, Cols: %d\n========\n", len(data), len(hdr))
	c := cardinality(indices)
	for i, h := range hdr {
		fmt.Printf("%v: %v\n", h, c[i])
	}
	fmt.Println("")
}

//Ingestion and indexing
func ingest(f io.Reader) (header []string, data [][]string, indices []map[string][]int, err error) {
	r := csv.NewReader(f)

	// handle header
	if header, err = r.Read(); err != nil {
		return
	}

	indices = make([]map[string][]int, len(header))
	var rowCount, colCount int = 0, len(header)
	for rec, err := r.Read(); err == nil; rec, err = r.Read() {
		if len(rec) != colCount {
			return nil, nil, nil, errors.Errorf("Expected Columns: %d. Got %d columns in row %d", colCount, len(rec), rowCount)
		}
		data = append(data, rec)
		for j, val := range rec {
			if indices[j] == nil {
				indices[j] = make(map[string][]int)
			}
			indices[j][val] = append(indices[j][val], rowCount)
		}
		rowCount++
	}
	return
}

// cardinality counts the number of unique values in a column.
// This assumes that the index i of indices represents a column.
func cardinality(indices []map[string][]int) []int {
	retVal := make([]int, len(indices))
	for i, m := range indices {
		retVal[i] = len(m)
	}
	return retVal
}

// mHandleErr is the error handler for the main function.
// If an error happens within the main function, it is not
// unexpected for a fatal error to be logged and for the program to immediately quit.
func mHandleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
