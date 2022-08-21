package workerpool

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"load_multiple_rows/database"
	"load_multiple_rows/entities"
	"load_multiple_rows/helper"
	"log"
	"os"
	"strconv"
	"sync"
)

type LoadWorker struct {
	Id string
}

func (w *LoadWorker) LoadCsv(
	db *database.DB, 
	path string,
	start_idx int,
	end_idx int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Run process
	var cnt int = 0
	var first bool = true
	var buildings []entities.Building

	csvFile, _ := os.Open(path)
	defer csvFile.Close()
	reader := csv.NewReader(bufio.NewReader(csvFile))
	for {
		line, error := reader.Read()

		// Skip header
		if first {
			first = false
			continue
		}

		// Break if error found
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		// Skip unnecessary rows
		if (cnt == end_idx) {
			break
		} else if (cnt < start_idx) {
			cnt = cnt + 1
			continue
		}

		// Prepare values
		bd := helper.ParseTransform(line)
		buildings = append(buildings, bd)

		// Add increment cnt
		cnt = cnt + 1
	}

	// Prepare sql statement
	sqlStr := "INSERT INTO building(BASE_BBL, MPLUTO_BBL, BIN, NAME, LSTMODDATE, LSTSTATTYPE, CNSTRCT_YR, DOITT_ID, HEIGHTROOF, FEAT_CODE, GROUNDELEV, GEOM_SOURCE) VALUES "
	vals := []interface{}{}
	for i, row := range buildings {
		sqlStr += "($" + strconv.Itoa(i * 12 + 1) + ", $" + 
					strconv.Itoa(i * 12 + 2) + ", $" + 
					strconv.Itoa(i * 12 + 3) + ", $" + 
					strconv.Itoa(i * 12 + 4) + ", $" + 
					strconv.Itoa(i * 12 + 5) + ", $" + 
					strconv.Itoa(i * 12 + 6) + ", $" + 
					strconv.Itoa(i * 12 + 7) + ", $" + 
					strconv.Itoa(i * 12 + 8) + ", $" + 
					strconv.Itoa(i * 12 + 9) + ", $" + 
					strconv.Itoa(i * 12 + 10) + ", $" + 
					strconv.Itoa(i * 12 + 11) + ", $" + 
					strconv.Itoa(i * 12 + 12) + "),"

		vals = append(vals, row.BASE_BBL, row.MPLUTO_BBL, row.BIN,
			row.NAME, row.LSTMODDATE, row.LSTSTATTYPE,
			row.CNSTRCT_YR, row.DOITT_ID, row.HEIGHTROOF,
			row.FEAT_CODE, row.GROUNDELEV, row.GEOM_SOURCE)
	}
	sqlStr = sqlStr[0: len(sqlStr) - 1] // Trim the last

	// Execute sql statement & value
	stmt, err := db.Prepare(sqlStr)
	if err != nil {
		log.Panic(err)
	}

	_, err = stmt.Exec(vals...)
	if err != nil {
		log.Panic(err)
	}

	fmt.Printf("Rows loaded from index %s to index %s at worker %s\n", 
				strconv.Itoa(start_idx), strconv.Itoa(end_idx), w.Id)
}