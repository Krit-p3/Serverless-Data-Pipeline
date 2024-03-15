from aws_utils.utils import get_delta_table
import polars as pl 


## --------------------

# Get Delta Table 

fact_trip = get_delta_table("fact_trip")
fact_trip = fact_trip.to_pandas()
fact_trip = pl.Dataframe(fact_trip)

dim_location = get_delta_table("dim_location")
dim_location = dim_location.to_pandas()
dim_location = pl.DataFrame(dim_location)


dim_payment = get_delta_table("dim_payment")
dim_payment = dim_payment.to_pandas()
dim_payment = pl.DataFrame(dim_payment)

dim_vendor = get_delta_table("dim_vendor")
dim_vendor = dim_vendor.to_pandas()
dim_vendor = pl.DataFrame(dim_vendor)


# Merged fact_trip with dim_location 

merged_df = (
    fact_trip
    .join(
        dim_location,
        letf_on="PULocationID",
        right_on="LocationID",
        how="left"
    )
    .rename(
        {
            "borough": "PU_borough",
            "zone": "PU_zone"
        }
    )
    .join(
        dim_location,
        left_on="DOLocationID",
        right_on="LocationID",
        how="left"
    )
    .rename(
        {
            "borough": "DO_borough",
            "zone": "DO_zone"
        }
    )
)

# Pickup hotspot 
pickup_hotspot = (
    merged_df.group_by("PU_borough")
    .agg(
        pl.len().alias("count")
    )
    .sort(
        "count", descending=True
    )
)

print(f"Pickup Hotspot:\n {pickup_hotspot}")

## ----------------------

# dropoff_hotspot 
dropoff_hotspot = (
    merged_df.group_by("DO_borough")
    .agg(
        pl.len().alias("count")
    )
    .sort("count", descending=True)
)
print(f"Dropoff Hotspot:\n {dropoff_hotspot}")

## -----------------------

# top 10 travel pattern

top_10_travel_pattern = (
    merged_df.group_by(
        ["PU_borough", "DO_borough"]
    )
    .agg(
        pl.len().alias("trip_count")
    )
    .sort("trip_count", descending=True)
    .head(10)
)

print(f"Top 10 Travel Pattern:\n {top_10_travel_pattern}")

## ------------------------

# Averrage trip Duration 

avg_trip_duration = (
    merged_df.group_by(
        ["PU_borough","PU_zone"]
    )
    .agg(
        pl.col("trip_duration_time_seconds").mean().alias("avg_trip_duration")
    )
    .with_columns(
       avg_trip_duration_minutes = pl.col("avg_trip_duration") / 60
    )
    .drop("avg_trip_duration")
    .sort("avg_trip_duration_minutes", descending=True)
)

print(f"Average Trip Duration:\n {avg_trip_duration}")

## ---------------------

# Payment preferences 

payment_preferences = (
    merged_df
    .join(dim_payment, on="payment_type", how="left")
    .group_by(
        ["PU_borough", "PU_zone", "payment"]
    )
    .agg(
        pl.len().alias("count")
    )
    .sort("count", descending=True)
)

print(f"Payment Preferences:\n {payment_preferences}")

## ----------------------

# Vendor analysis 

vendor_analysis = (
    merged_df
    .join(dim_vendor, on="VendorID", how="left")
    .group_by(
        ["VendorName", "PU_borough","PU_zone"]
    )
    .agg(
        pl.len().alias("count")
    )
    .sort("count", descending=True)
)

print(f"Vendor Analysis:\n {vendor_analysis}")