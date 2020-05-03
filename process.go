package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"github.com/mmcloughlin/geohash"
)

const (
    MIN_COUNT_OF_PEOPLE	= 10
    SMALL_GRID_BITS = 7 //76m grid
    MEDIUM_GRID_BITS = 6 //610m grid
    LARGE_GRID_BITS = 5 //2.4km grid
    VERY_LARGE_GRID_BITS = 4 //20km grid
    LARGEST_GRID_BITS = 3 //78km grid
)

var rows [][]string

func truncate(lat, lon float64, bits int) string {
    str:= geohash.Encode(lat, lon)
    return str[:bits+1]
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

	smallGrid := make(map[string]int)
	mediumGrid := make(map[string]int)
	largeGrid := make(map[string]int)
	veryLargeGrid := make(map[string]int)
	largestGrid := make(map[string]int)

	smallGridKeys := make(map[string]string)
	mediumGridKeys := make(map[string]string)
	largeGridKeys := make(map[string]string)
	veryLargeGridKeys := make(map[string]string)
	largestGridKeys := make(map[string]string)

	count := 0
	for {
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

		lat_str := record[5]
		lon_str := record[6]
		created_on_str := record[1]

		lat, _ := strconv.ParseFloat(lat_str, 64)
		lon, _ := strconv.ParseFloat(lon_str, 64)
		created_on := strings.Split(created_on_str, " ")[0]

		smallGridHash := truncate(lat, lon, SMALL_GRID_BITS)
		mediumGridHash := truncate(lat, lon, MEDIUM_GRID_BITS)
		largeGridHash := truncate(lat, lon, LARGE_GRID_BITS)
		veryLargeGridHash := truncate(lat, lon, VERY_LARGE_GRID_BITS)
		largestGridHash := truncate(lat, lon, LARGEST_GRID_BITS)

		//TODO: If files are on a per-day basis, this 
		//overhead can be avoided
		smallGridKey := smallGridHash + "," + created_on
		mediumGridKey := mediumGridHash + "," + created_on
		largeGridKey := largeGridHash + "," + created_on
		veryLargeGridKey := veryLargeGridHash + "," + created_on
		largestGridKey := largestGridHash + "," + created_on

		//Store the geohashes in maps to avoid recomputing
		lat_lon_key := lat_str + "," + lon_str
		smallGridKeys[lat_lon_key] = smallGridHash
		mediumGridKeys[lat_lon_key] = mediumGridHash
		smallGridKeys[lat_lon_key] = largeGridHash
		smallGridKeys[lat_lon_key] = veryLargeGridHash
		largestGridKeys[lat_lon_key] = largestGridHash

		if _, ok := smallGrid[smallGridKey]; ok {
		    smallGrid[smallGridKey]+=1
		} else {
		    smallGrid[smallGridKey] =1
		}

		if _, ok := mediumGrid[mediumGridKey]; ok {
		    mediumGrid[mediumGridKey]+=1
		} else {
		    mediumGrid[mediumGridKey] =1
		}

		if _, ok := largeGrid[largeGridKey]; ok {
		    largeGrid[largeGridKey]+=1
		} else {
		    largeGrid[largeGridKey] =1
		}

		if _, ok := veryLargeGrid[veryLargeGridKey]; ok {
		    veryLargeGrid[veryLargeGridKey]+=1
		} else {
		    veryLargeGrid[veryLargeGridKey] =1
		}

		if _, ok := largestGrid[largestGridKey]; ok {
		    largestGrid[largestGridKey]+=1
		} else {
		    largestGrid[largestGridKey] =1
		}

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
			//Remove lat
			record = RemoveIndex(record, 5)
			//Remove long
			record = RemoveIndex(record, 5)
			//Append a new field "geohash"
			record = append(record, "geohash")
			rows = append(rows, record)
			count += 1
			continue
		}

		if record[5] == "" || record[6] == "" {
			continue
		}

		lat_str := record[5]
		lon_str := record[6]
		created_on_str := record[1]

		created_on := strings.Split(created_on_str, " ")[0]

		lat_lon_key:= lat_str + "," + lon_str
		smallGridKey := smallGridKeys[lat_lon_key] + "," + created_on
		mediumGridKey := mediumGridKeys[lat_lon_key] + "," + created_on
		largeGridKey := largeGridKeys[lat_lon_key] + "," + created_on
		veryLargeGridKey := veryLargeGridKeys[lat_lon_key] + "," + created_on
		largestGridKey := largestGridKeys[lat_lon_key] + "," + created_on

		//Remove lat fields
		record = RemoveIndex(record, 5)
		//Remove lon field
		record = RemoveIndex(record, 5)

		if smallGrid[smallGridKey] >= MIN_COUNT_OF_PEOPLE {
		    record = append(record, smallGridKeys[lat_lon_key])
		} else if mediumGrid[mediumGridKey] >= MIN_COUNT_OF_PEOPLE {
		    record = append(record, mediumGridKeys[lat_lon_key])
		} else if largeGrid[largeGridKey] >= MIN_COUNT_OF_PEOPLE {
		    record = append(record, largeGridKeys[lat_lon_key])
		} else if veryLargeGrid[veryLargeGridKey] >= MIN_COUNT_OF_PEOPLE {
		    record = append(record, veryLargeGridKeys[lat_lon_key])
		} else if largestGrid[largestGridKey] >= MIN_COUNT_OF_PEOPLE {
		    record = append(record, largestGridKeys[lat_lon_key])
		} else {
		    record = append(record, largestGridKeys[lat_lon_key])
		}

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
