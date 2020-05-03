package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"math"
)

var MIN_COUNT_OF_PEOPLE int = 10
var rows [][]string

func truncate(x float64, n float64) float64 {
	return math.Floor(x*math.Pow(10,n))/math.Pow(10,n)
}

func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

func main() {
	// Open the file
	csvfile, err := os.Open("../data/output.csv")

	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)

	grids100m := make(map[string]int)
	grids1km := make(map[string]int)
	grids10km := make(map[string]int)
	grids100km := make(map[string]int)

	count := 0
	// Iterate through the records
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if count == 0 {
			count += 1
			continue
		}

		if record[5] == "" || record[6] == "" {
			continue
		}

		lat, _ := strconv.ParseFloat(record[5], 64)
		lon, _ := strconv.ParseFloat(record[6], 64)
		created_on := strings.Split(record[1], " ")[0]

		key100m:=fmt.Sprintf("%f", truncate(lat,3)) + "," + fmt.Sprintf("%f", truncate(lon,3)) + "," + created_on
		key1km:=fmt.Sprintf("%f", truncate(lat,2)) + "," + fmt.Sprintf("%f", truncate(lon,2)) + "," + created_on
		key10km:=fmt.Sprintf("%f", truncate(lat,1)) + "," + fmt.Sprintf("%f", truncate(lon,1)) + "," + created_on
		key100km:=fmt.Sprintf("%f", truncate(lat,0)+1) + "," + fmt.Sprintf("%f", truncate(lon,0)+1) + "," + created_on

		if _, ok := grids100m[key100m]; ok {
			grids100m[key100m]+=1
		} else {
			grids100m[key100m]=1
		}

		if _, ok := grids1km[key1km]; ok {
			grids1km[key1km]+=1
		} else {
			grids1km[key1km]=1
		}

		if _, ok := grids10km[key10km]; ok {
			grids10km[key10km]+=1
		} else {
			grids10km[key10km]=1
		}

		if _, ok := grids100km[key100km]; ok {
			grids100km[key100km]+=1
		} else {
			grids100km[key100km]=1
		}

		count += 1
	}

	_, err = csvfile.Seek(0, io.SeekStart)

	if err != nil {
		fmt.Println("Errored while rewinding")
	}

	r = csv.NewReader(csvfile)

	count = 0

	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		//Topic
		record = RemoveIndex(record, 16)
		//Partition
		record = RemoveIndex(record, 16)
		//Offset
		record = RemoveIndex(record, 16)
		//Raw Json
		record = RemoveIndex(record, 16)
		//lat_i
		record = RemoveIndex(record, 17)
		//lon_i
		record = RemoveIndex(record, 17)
		//ingestion_time
		record = RemoveIndex(record, 17)

		if count == 0 {
			rows = append(rows, record)
			count += 1
			continue
		}

		if record[5] == "" || record[6] == "" {
			continue
		}

		lat, _ := strconv.ParseFloat(record[5], 64)
		lon, _ := strconv.ParseFloat(record[6], 64)
		created_on := strings.Split(record[1], " ")[0]

		key100m:=fmt.Sprintf("%f", truncate(lat,3)) + "," + fmt.Sprintf("%f", truncate(lon,3)) + "," + created_on
		key1km:=fmt.Sprintf("%f", truncate(lat,2)) + "," + fmt.Sprintf("%f", truncate(lon,2)) + "," + created_on
		key10km:=fmt.Sprintf("%f", truncate(lat,1)) + "," + fmt.Sprintf("%f", truncate(lon,1)) + "," + created_on
		key100km:=fmt.Sprintf("%f", truncate(lat,0)+1) + "," + fmt.Sprintf("%f", truncate(lon,0)+1) + "," + created_on


		if grids100m[key100m] >= MIN_COUNT_OF_PEOPLE {
			splitKeys := strings.Split(key100m, ",")

			gridded_lat, _ := strconv.ParseFloat(splitKeys[0], 64)
			record[5] = fmt.Sprintf("%f", gridded_lat)

			gridded_lon, _ := strconv.ParseFloat(splitKeys[1], 64)
			record[6] = fmt.Sprintf("%f", gridded_lon)

		} else if grids1km[key1km] >= MIN_COUNT_OF_PEOPLE {
			splitKeys := strings.Split(key1km, ",")

			gridded_lat, _ := strconv.ParseFloat(splitKeys[0], 64)
			record[5] = fmt.Sprintf("%f", gridded_lat)

			gridded_lon, _ := strconv.ParseFloat(splitKeys[1], 64)
			record[6] = fmt.Sprintf("%f", gridded_lon)
		} else if grids10km[key10km] >= MIN_COUNT_OF_PEOPLE {
			splitKeys := strings.Split(key10km, ",")

			gridded_lat, _ := strconv.ParseFloat(splitKeys[0], 64)
			record[5] = fmt.Sprintf("%f", gridded_lat)

			gridded_lon, _ := strconv.ParseFloat(splitKeys[1], 64)
			record[6] = fmt.Sprintf("%f", gridded_lon)
		} else if grids100km[key100km] >= MIN_COUNT_OF_PEOPLE {
			splitKeys := strings.Split(key100km, ",")

			gridded_lat, _ := strconv.ParseFloat(splitKeys[0], 64)
			record[5] = fmt.Sprintf("%f", gridded_lat)

			gridded_lon, _ := strconv.ParseFloat(splitKeys[1], 64)
			record[6] = fmt.Sprintf("%f", gridded_lon)
		} else {
			record[5] = fmt.Sprintf("%f", truncate(lat, 0) + 1)
			record[6] = fmt.Sprintf("%f", truncate(lon, 0) + 1)
		}

		count += 1
		rows = append(rows, record)
	}

	f, err := os.Create("op-go.csv")
	if err != nil {
		log.Fatal(err)
	}
	err = csv.NewWriter(f).WriteAll(rows)
	f.Close()
	if err != nil {
		log.Fatal(err)
	}
}
