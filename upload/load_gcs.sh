#!/usr/bin/env bash

# Fail out if anything fails
set -e -o pipefail

# To do some timing
startTime="$(date -u +%s)"

cd /home/airflow/3107-pipeline/upload

for dir in */; do
    echo "$dir"
    cd $dir
    for json in *.json; do
        if [ -f "$json" ]; then
            echo
            echo "Preparing to copy $json to GCS..."
            gsutil cp $json gs://3107-cloud-storage/$json
            echo "$json copied to GCS ✅"
        fi
    done
    cd ../
done

echo
echo "All Loaded To GCS 🙌"
endTime="$(date -u +%s)"
duration="$(($endTime - $startTime))"
echo
echo "> $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed ⏱"
echo