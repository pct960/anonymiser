#!/usr/bin/env python3

import csv

count               =   {}
MIN_COUNT_OF_PEOPLE =   10

fields  =   [
                'key',
                'created_on',
                'did',
                'age',
                'gender',
                'lat',
                'lon',
                'past',
                'social',
                'social_when',
                'status',
                'symptom_past',
                'symptom_past_travel_social',
                'symptoms',
                'travel',
                'day',
                'confirm',
            ]

def process():
    
    with open('op-go.csv') as csv_file:
        reader = csv.DictReader(csv_file, fieldnames=fields)
        line_count  =   0

        for row in reader:
            if line_count == 0:
                line_count+=1
                continue
            else:
                key         =   (row["lat"], row["lon"], row["created_on"].split()[0])
                count[key]  =   count[key]  + 1 if (key     in count)   else 1
 
            line_count += 1

    outlier_count = 0
    for key in count:
        if count[key] < MIN_COUNT_OF_PEOPLE:
            outlier_count+=1

    print(outlier_count)

if __name__ == "__main__":
    process()
