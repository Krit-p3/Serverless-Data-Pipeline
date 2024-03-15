from aws_utils.utils import get_delta_table

data = get_delta_table("fact_trip")

print(data.head())