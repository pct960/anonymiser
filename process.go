package main

import (
	"encoding/csv"
	"fmt"
	"github.com/mmcloughlin/geohash"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	MIN_COUNT_OF_PEOPLE  = 10
	SMALL_GRID_BITS      = 7 //76m grid
	MEDIUM_GRID_BITS     = 6 //610m grid
	LARGE_GRID_BITS      = 5 //2.4km grid
	VERY_LARGE_GRID_BITS = 4 //20km grid
	LARGEST_GRID_BITS    = 3 //78km grid
)

var rows [][]string
var prevDay = time.Now().AddDate(0,0,-1)
var IP_FILE = "../non-gridded/covid_chat_" + prevDay.Format("02-01-06") + "_000"
var OP_FILE = "../gridded/covid_chat_details_realtime_" + prevDay.Format("02-01-2006") + ".csv"


func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

func main() {
	// Open the file
	csvfile, err := os.Open(IP_FILE)

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

	smallGridCache := make(map[string]string)
	mediumGridCache := make(map[string]string)
	largeGridCache := make(map[string]string)
	veryLargeGridCache := make(map[string]string)
	largestGridCache := make(map[string]string)

	//PASS 1: Compute all grids and their respective member count
	count := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		//Skip a record if there are errors in parsing it
		if err != nil {
			//log.Fatal(err)
			fmt.Println(err)
			continue
		}

		//Don't process the first line
		if count == 0 {
			count += 1
			continue
		}

		//Skip a record if lat, long is null
		if record[5] == "" || record[6] == "" {
			continue
		}

		lat_str := record[5]
		lon_str := record[6]

		lat, _ := strconv.ParseFloat(lat_str, 64)
		lon, _ := strconv.ParseFloat(lon_str, 64)

		//Get the geohash for all grid sizes from the lat,long pair
		smallGridHash := geohash.EncodeWithPrecision(lat, lon, SMALL_GRID_BITS)
		mediumGridHash := geohash.EncodeWithPrecision(lat, lon, MEDIUM_GRID_BITS)
		largeGridHash := geohash.EncodeWithPrecision(lat, lon, LARGE_GRID_BITS)
		veryLargeGridHash := geohash.EncodeWithPrecision(lat, lon, VERY_LARGE_GRID_BITS)
		largestGridHash := geohash.EncodeWithPrecision(lat, lon, LARGEST_GRID_BITS)

		//Store the geohashes in maps to avoid recomputing
		lat_lon_key := lat_str + "," + lon_str
		smallGridCache[lat_lon_key] = smallGridHash
		mediumGridCache[lat_lon_key] = mediumGridHash
		largeGridCache[lat_lon_key] = largeGridHash
		veryLargeGridCache[lat_lon_key] = veryLargeGridHash
		largestGridCache[lat_lon_key] = largestGridHash

		/*TODO: Some optimisation is possible here.
		*If the count reaches MIN_COUNT_OF_PEOPLE during the first pass,
		*there's no need to go over all records in the second pass
		*/

		if _, ok := smallGrid[smallGridHash]; ok {
			smallGrid[smallGridHash] += 1
		} else {
			smallGrid[smallGridHash] = 1
		}

		if _, ok := mediumGrid[mediumGridHash]; ok {
			mediumGrid[mediumGridHash] += 1
		} else {
			mediumGrid[mediumGridHash] = 1
		}

		if _, ok := largeGrid[largeGridHash]; ok {
			largeGrid[largeGridHash] += 1
		} else {
			largeGrid[largeGridHash] = 1
		}

		if _, ok := veryLargeGrid[veryLargeGridHash]; ok {
			veryLargeGrid[veryLargeGridHash] += 1
		} else {
			veryLargeGrid[veryLargeGridHash] = 1
		}

		if _, ok := largestGrid[largestGridHash]; ok {
			largestGrid[largestGridHash] += 1
		} else {
			largestGrid[largestGridHash] = 1
		}
	}

	//Rewind file to avoid another I/O overhead
	_, err = csvfile.Seek(0, io.SeekStart)

	if err != nil {
		fmt.Println("Errored while rewinding")
	}

	r = csv.NewReader(csvfile)

	//PASS 2: Assign smallest grid to [lat, long] such that member_count >= MIN_COUNT_OF_PEOPLE  

	count = 0
	smallGridCount := 0
	mediumGridCount := 0
	largeGridCount :=0
	veryLargeGridCount := 0
	largestGridCount := 0
	outlierCount := 0

	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			//log.Fatal(err)
			fmt.Println(err)
			continue
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

		lat_lon_key := lat_str + "," + lon_str
		smallGridHash := smallGridCache[lat_lon_key]
		mediumGridHash := mediumGridCache[lat_lon_key]
		largeGridHash := largeGridCache[lat_lon_key]
		veryLargeGridHash := veryLargeGridCache[lat_lon_key]
		largestGridHash := largestGridCache[lat_lon_key]

		if smallGrid[smallGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(smallGridCache[lat_lon_key])
			record[5] = fmt.Sprintf("%f", decoded_lat)
			record[6] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, smallGridHash)
			smallGridCount += 1
		} else if mediumGrid[mediumGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(mediumGridCache[lat_lon_key])
			record[5] = fmt.Sprintf("%f", decoded_lat)
			record[6] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, mediumGridHash)
			mediumGridCount += 1
		} else if largeGrid[largeGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(largeGridCache[lat_lon_key])
			record[5] = fmt.Sprintf("%f", decoded_lat)
			record[6] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, largeGridHash)
			largeGridCount += 1
		} else if veryLargeGrid[veryLargeGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(veryLargeGridCache[lat_lon_key])
			record[5] = fmt.Sprintf("%f", decoded_lat)
			record[6] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, veryLargeGridHash)
			veryLargeGridCount += 1
		} else if largestGrid[largestGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(largestGridCache[lat_lon_key])
			record[5] = fmt.Sprintf("%f", decoded_lat)
			record[6] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, largestGridHash)
			largestGridCount += 1
		} else {
			decoded_lat, decoded_lon := geohash.DecodeCenter(largestGridCache[lat_lon_key])
			record[5] = fmt.Sprintf("%f", decoded_lat)
			record[6] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, largestGridHash)
			outlierCount += 1
		}

		rows = append(rows, record)
		count += 1
	}

	fmt.Println("76m=", smallGridCount)
	fmt.Println("610m=", mediumGridCount)
	fmt.Println("2.4km=", largeGridCount)
	fmt.Println("20km=", veryLargeGridCount)
	fmt.Println("78km=", largestGridCount)
	fmt.Println("Outliers=", outlierCount)
	fmt.Println("Total=", count)

	//Write to the output file
	f, err := os.Create(OP_FILE)
	if err != nil {
		log.Fatal(err)
	}
	err = csv.NewWriter(f).WriteAll(rows)
	_ = f.Close()
	if err != nil {
		log.Fatal(err)
	}
}
