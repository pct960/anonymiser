package main

import (
	"encoding/csv"
	"encoding/json"
	"math/rand"
	"io/ioutil"
	"fmt"
	"github.com/mmcloughlin/geohash"
	"io"
	"log"
	"os"
	"strconv"
	"time"
	"github.com/lucasjones/reggen"
)

const (
	MIN_COUNT_OF_PEOPLE	= 10
	SMALL_GRID_BITS		= 7 //76m grid
	MEDIUM_GRID_BITS	= 6 //610m grid
	LARGE_GRID_BITS		= 5 //2.4km grid
	VERY_LARGE_GRID_BITS	= 4 //20km grid
	LARGEST_GRID_BITS	= 3 //78km grid
	ADD_NOISE		= true
)

type schema struct {
	names map[string]interface{}
}

const (
	DID				= 0
	PHONENUMBER			= 1
	FULLNAME			= 2
	AGE				= 3
	GENDER				= 4
	PROFESSION			= 5
	LAT				= 6
	LON				= 7
	LOCATION_UPDATED_AT		= 8
	STATUS				= 9
	SYMPTOM_PAST			= 10
	SYMPTOM_PAST_TRAVEL_SOCIAL	= 11
	SYMPTOMS			= 12
	PLATFORM			= 13
	IS_COMORBID			= 14
	POSTALCODE			= 15
	SUB_DISTRICT			= 16
	AREA				= 17
	GEO_STATE			= 18
	GEO_DISTRICT			= 19
	DISTRICT_CODE			= 20
	AUTH_STATUS			= 21
	BLUETOOTH_TRACED		= 22
)

var rows [][]string
var prevDay = time.Now().AddDate(0,0,-1)
var IP_FILE = "bangalore_suspects.csv"
var OP_FILE = "bangalore_suspects_anonymised.csv"

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

		lat_str := record[LAT]
		lon_str := record[LON]

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

	rand.Seed(time.Now().Unix())

	namesJson, err := os.Open("names.json")
	if err != nil {
		fmt.Println(err)
	}

	byteValue, _ := ioutil.ReadAll(namesJson)

	var result schema
	json.Unmarshal([]byte(byteValue), &result.names)

	maleFirstNames := result.names["male_first_names"].([]interface{})
	femaleFirstNames := result.names["female_first_names"].([]interface{})
	lastNames := result.names["last_names"].([]interface{})

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

		if count == 0 {
			record = append(record, "geohash")
			rows = append(rows, record)
			count += 1
			continue
		}

		lat_str := record[LAT]
		lon_str := record[LON]

		lat_lon_key := lat_str + "," + lon_str
		smallGridHash := smallGridCache[lat_lon_key]
		mediumGridHash := mediumGridCache[lat_lon_key]
		largeGridHash := largeGridCache[lat_lon_key]
		veryLargeGridHash := veryLargeGridCache[lat_lon_key]
		largestGridHash := largestGridCache[lat_lon_key]

		if smallGrid[smallGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(smallGridCache[lat_lon_key])
			record[LAT] = fmt.Sprintf("%f", decoded_lat)
			record[LON] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, smallGridHash)
			smallGridCount += 1
		} else if mediumGrid[mediumGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(mediumGridCache[lat_lon_key])
			record[LAT] = fmt.Sprintf("%f", decoded_lat)
			record[LON] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, mediumGridHash)
			mediumGridCount += 1
		} else if largeGrid[largeGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(largeGridCache[lat_lon_key])
			record[LAT] = fmt.Sprintf("%f", decoded_lat)
			record[LON] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, largeGridHash)
			largeGridCount += 1
		} else if veryLargeGrid[veryLargeGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(veryLargeGridCache[lat_lon_key])
			record[LAT] = fmt.Sprintf("%f", decoded_lat)
			record[LON] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, veryLargeGridHash)
			veryLargeGridCount += 1
		} else if largestGrid[largestGridHash] >= MIN_COUNT_OF_PEOPLE {
			decoded_lat, decoded_lon := geohash.DecodeCenter(largestGridCache[lat_lon_key])
			record[LAT] = fmt.Sprintf("%f", decoded_lat)
			record[LON] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, largestGridHash)
			largestGridCount += 1
		} else {
			decoded_lat, decoded_lon := geohash.DecodeCenter(largestGridCache[lat_lon_key])
			record[LAT] = fmt.Sprintf("%f", decoded_lat)
			record[LON] = fmt.Sprintf("%f", decoded_lon)
			record = append(record, largestGridHash)
			outlierCount += 1
		}

		if ADD_NOISE {
			num, _ := reggen.Generate("^(7|8|9){1}[0-9]{9}$", 1)
			record[PHONENUMBER] = num
			name := ""

			if record[GENDER] == "Male" {
				name = maleFirstNames[rand.Int() % len(maleFirstNames)].(string) + " " + lastNames[rand.Int() % len(lastNames)].(string)
			} else {
				name = femaleFirstNames[rand.Int() % len(femaleFirstNames)].(string) + " " + lastNames[rand.Int() % len(lastNames)].(string)

			}

			record[FULLNAME] = name
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
