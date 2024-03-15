lint: 
	poetry run ruff check --fix .

format: 
	poetry run ruff format .

upload_s3:
	bash download_parquet.sh 

delta_table:
	poetry run python src/create_deltatable.py

examine_delta:
	poetry run python src/aws_utils/examine.py -t ${table_name}

