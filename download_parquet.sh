# !/bin/bash 

# Download parquet file and upload to Aws s3 bucket

output="src/url_to_download.txt"
years=$(seq 2014 2023)
months=$(seq 1 12)
caps="yellow green" 
# Loop through each year
for cap in $caps; do
    for year in $year; do 
        for month in $months; do 
            poetry run python src/get_url.py --type $cap --year $year --month $month --output $output
            echo "URLs have been write to $output"
            poetry run python src/get_data/upload_to_s3.py
        done 
    done 
done 

