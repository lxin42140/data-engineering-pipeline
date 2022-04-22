#!/usr/bin/env bash

# Fail out if anything fails
set -e -o pipefail

# To do some timing
startTime="$(date -u +%s)"

cd /home/airflow/3107-pipeline/upload

for dir in */; do
    cd $dir
    for json in *.json; do
        if [ -f "$json" ]; then
            echo
            echo "Preparing to load $json to BigQuery..."
            tableName=$(echo $json | cut -d. -f1)
            bq load --project_id=project-346314  \
                --autodetect=true \
                --source_format=NEWLINE_DELIMITED_JSON  \
                3107.$tableName \
                gs://3107-cloud-storage/$json
            echo "$tableName loaded to BigQuery ✅"
        fi
    done
    cd ../
done

echo
echo "All Loaded To BigQuery 🙌"
endTime="$(date -u +%s)"
duration="$(($endTime - $startTime))"
echo
echo "> $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed ⏱"
echo