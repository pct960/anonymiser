#!/usr/bin/env python3

import csv
import math

#Fields of dynamo.covid_chat_details_realtime
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
                'topic',
                'partition',
                'offset',
                'raw_json',
                'confirm',
                'lat_i',
                'lon_i',
                'ingestion_time'
            ]

def process():
    
    line_count = 0
    with open('../data/output.csv') as csv_file:

        reader = csv.DictReader(csv_file, fieldnames=fields)

        for row in reader:
            line_count += 1

    print(line_count)

process()
