#!/usr/bin/env python3

import csv
import math

grids100m   =   {}
grids1km    =   {}
grids10km   =   {}
grids100km  =   {}

MIN_COUNT_OF_PEOPLE =   10

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

def truncate(f, n):
    return math.floor(f * 10 ** n) / 10 ** n

def process():
    
    with open('output.csv') as csv_file:

        #Initiliase reader and writer for the csv file
        reader = csv.DictReader(csv_file, fieldnames=fields)

        '''
        Pass 1: Compute the 100m x 100m grids and count 
        the number of points in the respective grids
        '''
        line_count  =   0
        for row in reader:

            #Skip processing the first row since they are column names
            if line_count == 0:
                line_count+=1
                continue

            #Skip processing rows without lat and long values
            if (not row["lat"]) or (not row["lon"]):
                line_count+=1
                continue

            else:
                #Get lat, lon and created_on fields
                lat         =   float(row["lat"])
                lon         =   float(row["lon"])
                created_on  =   row["created_on"].split()[0] 
                
                #Compute keys by gridding from 100m x 100m - 100km x 100km
                key100m     =   (truncate(lat,3), truncate(lon,3), created_on)
                key1km      =   (truncate(lat,2), truncate(lon,2), created_on)
                key10km     =   (truncate(lat,1), truncate(lon,1), created_on)
                key100km    =   (truncate(lat,0)+1, truncate(lon,0)+1, created_on)

                #Construct initial membership table
                grids100m[key100m]      =   grids100m[key100m]  + 1 if (key100m     in grids100m)   else 1
                grids1km[key1km]        =   grids1km[key1km]    + 1 if (key1km      in grids1km)    else 1
                grids10km[key10km]      =   grids10km[key10km]  + 1 if (key10km     in grids10km)   else 1
                grids100km[key100km]    =   grids100km[key100km]+ 1 if (key100km    in grids100km)  else 1
 
            line_count += 1

        '''
        Pass 2: Go through all points again to check if they
        belong to a bbox with at least 10 chats on that day
        '''

        #Seek to the beginning of the file to avoid another I/O
        csv_file.seek(0)
        next(reader)

        #Use a list to avoid update I/O
        update_list =   []

        line_count  =   0

        for row in reader:


            if line_count == 0:
                line_count+=1
                continue

            if (not row["lat"]) or (not row["lon"]):
                line_count+=1
                continue

            else:
                lat         =   float(row["lat"])
                lon         =   float(row["lon"])
                created_on  =   row["created_on"].split()[0] 

                key100m     =   (truncate(lat,3), truncate(lon,3), created_on)
                key1km      =   (truncate(lat,2), truncate(lon,2), created_on)
                key10km     =   (truncate(lat,1), truncate(lon,1), created_on)
                key100km    =   (truncate(lat,0)+1, truncate(lon,0)+1, created_on)

                if grids100m[key100m]       >=  MIN_COUNT_OF_PEOPLE:
                    row["lat"]  =   key100m[0]
                    row["lon"]  =   key100m[1]
                elif grids1km[key1km]       >=  MIN_COUNT_OF_PEOPLE:
                    row["lat"]  =   key1km[0]
                    row["lon"]  =   key1km[1]
                elif grids10km[key10km]     >=  MIN_COUNT_OF_PEOPLE:
                    row["lat"]  =   key10km[0]
                    row["lon"]  =   key10km[1]
                elif grids100km[key100km]   >=  MIN_COUNT_OF_PEOPLE:
                    row["lat"]  =   key100km[0]
                    row["lon"]  =   key100km[1]
                else:
                    row["lat"]  =   truncate(lat,0) + 1
                    row["lon"]  =   truncate(lon,0) + 1

            row.pop("raw_json")
            row.pop("lat_i")
            row.pop("lon_i")
            row.pop("ingestion_time")
            row.pop("topic")
            row.pop("partition")
            row.pop("offset")
            update_list.append(row)

            line_count += 1

    fields.remove("raw_json")
    fields.remove("lat_i")
    fields.remove("lon_i")
    fields.remove("ingestion_time")
    fields.remove("topic")
    fields.remove("partition")
    fields.remove("offset")

    with open('chat_details_gridded.csv', 'w') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(fields)

        for row in update_list:
            wr.writerow(dict(row).values())

if __name__ == "__main__":
    process()
