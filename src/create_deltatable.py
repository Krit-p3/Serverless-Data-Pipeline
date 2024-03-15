from aws_utils.utils import write_delta_table
import pandas as pd 

csv_path = "data/taxi_zones.csv"

zone_df = pd.read_csv(csv_path)

# Select specific columns zone, LocationID , borough
zone_df = zone_df[['zone','LocationID','borough']]

# Name delta table
zone_path = "dim_location"
# Write to s3 
write_delta_table(zone_df, zone_path)

##Create dim_vendor Table
dim_vendor = pd.DataFrame(
    {
        "VendorID": [1, 2],
        "VendorName": ["Creative Mobile Technologies", "VeriFone Inc."],
    }
)

## ----------------------

vendor_path = "dim_vendor"
write_delta_table(dim_vendor, vendor_path)


## ----------------------

# Rate_table
rate_table = pd.DataFrame(
    {
        "RateCodeID": [1, 2, 3, 4, 5, 6],
        "RateName": [
            "Standard rate",
            "JFK",
            "Newark",
            "Nassau or Westchester",
            "Negotiated fare",
            "Group ride",
        ],
    }
)
rate_path = "dim_rate"
write_delta_table(rate_table, rate_path)

## ----------------------

# Payment_table
dim_payment = pd.DataFrame(
    {
        "payment_type": [1, 2, 3, 4, 5, 6],
        "payment": [
            "Credit card",
            "Cash",
            "No charge",
            "Dispute",
            "Unknown",
            "Voided trip",
        ]
    }
)

payment_path = "dim_payment"
write_delta_table(dim_payment, payment_path)