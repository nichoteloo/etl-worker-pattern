package helper

import (
	"errors"
	"fmt"
	"load_multiple_rows/entities"
	"log"
	"reflect"
	"strconv"
	"time"
)

// Check if null value exist
func sanitizeConv(data string, target string) interface{} {
	if (target == "float") {
		if (data != "") {
			dataConv, err := strconv.ParseFloat(data, 64)
			if err != nil {
				log.Panic(err)
			}

			return dataConv
		} else {
			return 0.0
		}
	} else if (target == "date") {
		const dateformat = "01/02/2006 03:04:05 PM"
		if (data != "") {
			dataConv, err := time.Parse(dateformat, data)
			if err != nil {
				log.Panic(err)
			}

			return dataConv
		} else {
			dataConv, err := time.Parse(dateformat, "01/01/1990 00:00:00 AM")
			if err != nil {
				log.Panic(err)
			}

			return dataConv 
		}
	} else {
		return data
	}
}

// Map to struct
func fillStruct(m map[string]interface{}, bd *entities.Building) error {
	for k, v := range m {
		structValue := reflect.ValueOf(bd).Elem()
		structFieldValue := structValue.FieldByName(k)

		if !structFieldValue.IsValid() { // Check if key is exist
			return fmt.Errorf("no such field: %s in obj", k)
		}

		if !structFieldValue.CanSet() { // Check if key is able to set
			return fmt.Errorf("cannot set %s field value", k)
		}

		structFieldType := structFieldValue.Type() 
		val := reflect.ValueOf(v)
		if structFieldType != val.Type() { // Check if value type is correct
			return errors.New("provided value type didn't match obj field type")
		}

		structFieldValue.Set(val)
	}

	return nil
}

func ParseTransform(line []string) entities.Building {
	// Perform struct mapping
	myData := make(map[string]interface{})

	myData["BASE_BBL"] = sanitizeConv(line[0], "string")
	myData["MPLUTO_BBL"] = sanitizeConv(line[1], "string")
	myData["BIN"] = sanitizeConv(line[2], "float")
	myData["NAME"] = sanitizeConv(line[3], "string")
	myData["LSTMODDATE"] = sanitizeConv(line[4], "date")
	myData["LSTSTATTYPE"] = sanitizeConv(line[5], "string")
	myData["CNSTRCT_YR"] = sanitizeConv(line[6], "float")
	myData["DOITT_ID"] = sanitizeConv(line[7], "float")
	myData["HEIGHTROOF"] = sanitizeConv(line[8], "float")
	myData["FEAT_CODE"] = sanitizeConv(line[9], "float")
	myData["GROUNDELEV"] = sanitizeConv(line[10], "float")
	myData["GEOM_SOURCE"] = sanitizeConv(line[11], "string")

	bd := &entities.Building{}
	err := fillStruct(myData, bd)
	if err != nil {
		log.Panic(err)
	}

	return *bd
}