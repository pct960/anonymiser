#!/usr/bin/env python3

import pandas as pd

chat_data   =   pd.read_csv("../data/output.csv")	

grids100m   =   {}
grids1km    =   {}
grids10km   =   {}
grids100km  =   {}

for index, row in chat_data.iterrows():

    if index == 0:
        continue

    if (not row["lat"]) or (not row["lon"]):
        continue

    lat = float(row["lat"])
    lon = float(row["lon"])
    created_on  =   row["created_on"].split()[0] 

    #Compute keys by gridding from 100m x 100m - 100km x 100km
    key100m     =   (round(lat,3), round(lon,3), created_on)
    key1km      =   (round(lat,2), round(lon,2), created_on)
    key10km     =   (round(lat,1), round(lon,1), created_on)
    key100km    =   (round(lat,3)+1, round(lon,3)+1, created_on)

    #Construct initial membership table
    grids100m[key100m]      =   grids100m[key100m]  + 1 if (key100m     in grids100m)   else 1
    grids1km[key1km]        =   grids1km[key1km]    + 1 if (key1km      in grids1km)    else 1
    grids10km[key10km]      =   grids10km[key10km]  + 1 if (key10km     in grids10km)   else 1
    grids100km[key100km]    =   grids100km[key100km]+ 1 if (key100km    in grids100km)  else 1

for index, row in chat_data.iterrows():

    if index == 0:
        continue

    if (not row["lat"]) or (not row["lon"]):
        continue

    lat = float(row["lat"])
    lon = float(row["lon"])
    created_on  =   row["created_on"].split()[0] 

    #Compute keys by gridding from 100m x 100m - 100km x 100km
    key100m     =   (round(lat,3), round(lon,3), created_on)
    key1km      =   (round(lat,2), round(lon,2), created_on)
    key10km     =   (round(lat,1), round(lon,1), created_on)
    key100km    =   (round(lat,3)+1, round(lon,3)+1, created_on)

    if grids100m[key100m]      >=  10:
        chat_data.at[index, "lat"] = round(lat, 3)
        chat_data.at[index, "lon"] = round(lon, 3)
    elif grids1km[key1km]      >=  10:
        chat_data.at[index, "lat"] = round(lat, 2)
        chat_data.at[index, "lon"] = round(lon, 2)
    elif grids10km[key10km]    >=  10:
        chat_data.at[index, "lat"] = round(lat, 1)
        chat_data.at[index, "lon"] = round(lon, 1)
    elif grids100km[key100km]  >=  10:
        chat_data.at[index, "lat"] = round(lat, 3) + 1
        chat_data.at[index, "lon"] = round(lon, 3) + 1

chat_data.to_csv("op.csv")
