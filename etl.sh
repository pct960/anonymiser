#!/bin/bash

cd ~/anonymiser/

filename="covid_chat_details_realtime_"$(date -d 'yesterday' '+%d-%m-%Y')".csv"
filepath="gridded/"$filename

aws s3 sync s3://aarogya-setu non-gridded/
./chat
aws s3api put-object --bucket aarogya-setu-anonymised --key $filename --body $filepath
