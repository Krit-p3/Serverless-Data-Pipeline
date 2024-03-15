import uuid 
import polars as pl
from deltalake import write_deltalake 


def read_data(data):
    return pl.read_parquet(data)



def data_wrangling(df , values):

    
    def generate_uuid():
        return str(uuid.uuid4())
    def generate_time_id():
        return str(uuid.uuid1())



    df = (
        df
        .with_columns(
            pl.col("improvement_surcharge").cast(pl.Float32),
            pl.col("congestion_surcharge").cast(pl.Float32),
            pl.col("airport_fee").cast(pl.Int8),
            pl.col("payment_type").cast(pl.Int8),
            pickup_year=pl.col("tpep_pickup_datetime").dt.year(),
            pickup_month=pl.col("tpep_pickup_datetime").dt.month(),
            pickup_day=pl.col("tpep_pickup_datetime").dt.day(),
            pickup_time=pl.col("tpep_pickup_datetime").dt.to_string("%T").str.to_datetime(format="%H:%M:%S"),
            dropoff_year=pl.col("tpep_dropoff_datetime").dt.year(),
            dropoff_month=pl.col("tpep_dropoff_datetime").dt.month(),
            dropoff_day=pl.col("tpep_dropoff_datetime").dt.day(),
            dropoff_time=pl.col("tpep_dropoff_datetime").dt.to_string("%T").str.to_datetime(format="%H:%M:%S")
        )
        .with_columns(
            trip_duration_time_seconds = pl.col("dropoff_time") - pl.col("pickup_time")
        )
        .with_columns(
            pl.col("trip_duration_time_seconds").dt.total_seconds()
        )
        .with_columns(
            taxi_type = pl.lit(values)
        )
        .with_row_index("id")
            .with_columns(
                trip_id = pl.col("id").map_elements(lambda x: generate_uuid()),
                time_id = pl.col("id").map_elements(lambda x: generate_time_id()                )
        )
        .drop("id")

    )

    return df

def clean_data(df):

    df = (
            df
            .with_columns(
                pl.col("passenger_count").fill_null(1),
                pl.col("congestion_surcharge").fill_null(0),
                pl.col("airport_fee").fill_null(0)
            )
            .filter(pl.col("trip_duration_time_seconds") > 0)
            .drop(["tpep_pickup_datetime", "tpep_dropoff_datetime"])
            .drop_nulls()
            .unique(keep="first",maintain_order=True)

        )

    return df



def fact_trip_table(path, df):

    df = df.select(
            pl.col([
            "trip_id",
            "time_id",
            "VendorID",
            "payment_type",
            "taxi_type",
            "passenger_count",
            "trip_distance",
            "store_and_fwd_flag",
            "DOLocationID",
            "PULocationID",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
            "trip_duration_time_seconds",
            "RatecodeID"
        ])
    )

    try:
        df = df.to_pandas()
        write_deltalake(path,df, mode="append")
        print("fact_trip_table have been update!")

    except Exception as e:
        print(f"Error occurred: {str(e)}")


def dim_time_table(path, df):

    df = df.select(
        pl.col([
            "time_id",
            "pickup_year",
            "pickup_month",
            "pickup_day",
            "pickup_time",
            "dropoff_year",
            "dropoff_month",
            "dropoff_day",
            "dropoff_time"
        ])
    )

    try:
        df = df.to_pandas()
        write_deltalake(path, df, mode="append")
        print("dim_time_table have been update!")

    except Exception as e:
        print(f"Error occurred: {str(e)}")


