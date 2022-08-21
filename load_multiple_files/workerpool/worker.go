package workerpool

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"load_multiple_files/database"
	"load_multiple_files/entities"
	"load_multiple_files/helper"
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
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Run process
	var first bool = true
	var buildings []entities.Building

	csvFile, _ := os.Open(path)
	defer csvFile.Close()

	reader := csv.NewReader(bufio.NewReader(csvFile))
	lines, err := reader.ReadAll()
	if err != nil {
		log.Panic(err)
	}

	for _, line := range lines {
		if first {
			first = false
			continue // Skip header
		}

		// Prepare values (map one by one)
		bd := helper.ParseTransform(line)
		buildings = append(buildings, bd)
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

	res, err := stmt.Exec(vals...)
	if err != nil {
		log.Panic(err)
	}

	rowCnt, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s rows loaded from worker %s\n", strconv.Itoa(int(rowCnt)), w.Id)
}