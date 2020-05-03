from haversine import haversine, Unit
import csv

with open('newFile.csv', newline='') as f:
    reader = csv.reader(f)
    data = list(reader)

for src_point in data:
    distance_list = []
    for dest_point in data:
        if src_point[2] == dest_point[2]:
            distance_list.append(haversine((float(src_point[0]), float(src_point[1])), (float(dest_point[0]), float(dest_point[1]))))
        else:
            continue

    if min(distance_list) > 100:
        print(src_point)
