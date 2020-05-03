#!/usr/bin/env python3

import csv
import Geohash

fields  =   [
                'key',
                'created_on',
                'did',
                'age',
                'gender',
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
                'geohash'
            ]

def process():
    
    update_list = []
    with open('op-go.csv') as csv_file:
        reader = csv.DictReader(csv_file, fieldnames=fields)
        line_count  =   0

        for row in reader:
            if line_count == 0:
                row.pop("geohash")
                row.update({'lat': 'lat'})
                row.update({'lon':'lon'})
                line_count+=1
                update_list.append(row)
                continue
            else:
                hashed = row["geohash"]
                lat = Geohash.decode(hashed)[0]
                lon = Geohash.decode(hashed)[1]
                row.pop("geohash")
                row.update({'lat':lat})
                row.update({'lon':lon})

            update_list.append(row)

    with open('converted.csv', 'w') as myfile:
        wr = csv.writer(myfile)
        for row in update_list:
            wr.writerow(dict(row).values())

process()
