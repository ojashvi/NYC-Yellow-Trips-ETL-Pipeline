from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import sys

def main():
    # 1. Initialize the Spark Session (The "Engine")
    print("Starting Spark Session...")
    spark = SparkSession.builder \
        .appName("NYCTaxi_Data_Processing") \
        .getOrCreate()

    # 2. Read the Parquet File
    execution_month = sys.argv[1]
    file_path = f'/opt/airflow/data/raw/yellow_tripdata_{execution_month}.parquet'
    print(f"Reading data from {file_path}...")
    df = spark.read.parquet(file_path)

    # (Optional) Print original shape
    print(f"Original Row Count: {df.count()}")
    print(f"Column Count: {len(df.columns)}")

    # 3. Clean the Data 
    # Equivalent to: df.dropna()
    df = df.dropna()

    # Equivalent to: df = df[(df['fare_amount'] >= 0) & (df['passenger_count'] > 0)]
    df_cleaned = df.filter((col('fare_amount') >= 0) & (col('passenger_count') > 0))
    print(f"Cleaned Row Count: {df_cleaned.count()}")

    # 4. Aggregate the Data
    # Equivalent to: df.groupby(['VendorID'])['total_amount'].sum().reset_index()
    vendor_sum = df_cleaned.groupBy('VendorID') \
                           .agg(_sum('total_amount').alias('total_amount'))

    # 5. Display the Results
    print("Total Amount by Vendor:")
    vendor_sum.show()
    
    # 6. Save the output (Uncomment this when you want to save the results to disk)
    # vendor_sum.write.mode("overwrite").parquet("/content/processed_vendor_sum.parquet")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":

    main()