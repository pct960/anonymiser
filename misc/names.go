package main

import (
	"encoding/json"
	"math/rand"
	"os"
	"fmt"
	"io/ioutil"
	"time"
)

type schema struct {
	names map[string]interface{}
}

func main() {

	rand.Seed(time.Now().Unix())

	names, err := os.Open("names.json")
	if err != nil {
		fmt.Println(err)
	}

	byteValue, _ := ioutil.ReadAll(names)

	var result schema
	json.Unmarshal([]byte(byteValue), &result.names)

	maleFirstNames := result.names["male_first_names"].([]interface{})
	lastNames := result.names["last_names"].([]interface{})

	maleName := maleFirstNames[rand.Int() % len(maleFirstNames)].(string) + " " + lastNames[rand.Int() % len(lastNames)].(string)

	fmt.Println(maleName)
}
