import requests
import geohash
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pandas as pd
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Restaurant and Weather Data Processing") \
    .getOrCreate()

restaurant_data_path = "data/restaurant_csv/part-0.csv"
restaurant_df = spark.read.csv(restaurant_data_path, header=True, inferSchema=True)

# Load weather data (Parquet)
weather_data_path = "data/weather/part-00140-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"  # Replace with your Parquet file path
weather_df = spark.read.parquet(weather_data_path)

restaurant_df.show(5)
weather_df.show(5)

# OpenCage API endpoint and API key (replace with your actual key)
opencage_api_url = "https://api.opencagedata.com/geocode/v1/json"
api_key = os.getenv("OPENCAGE_API_KEYpip")
#api_key_broadcast = spark.sparkContext.broadcast(api_key)

# Function to fetch latitude and longitude from OpenCage API based on address
def get_lat_lon_from_opencage(address):
    params = {
        "q": address,
        "key": api_key
    }
    response = requests.get(opencage_api_url, params=params)
    data = response.json()

    if data['results']:
        lat = data['results'][0]['geometry']['lat']
        lon = data['results'][0]['geometry']['lng']
        return lat, lon
    else:
        return None, None


# UDF to generate geohash from latitude and longitude
def generate_geohash(lat, lon):
    if lat is not None and lon is not None:
        return geohash.encode(lat, lon, precision=4)
    return None


geohash_udf = udf(generate_geohash, StringType())


# Fill null latitudes and longitudes with OpenCage API
def fill_missing_lat_lon(df):
    # Filter rows where latitude and longitude are missing
    missing_lat_lon = df.filter((df['lat'].isNull()) | (df['lng'].isNull()))

    # Use OpenCage API to get missing values (this is a simplified example)
    for row in missing_lat_lon.collect():
        address = row['city']
        lat, lon = get_lat_lon_from_opencage(address) #api_key_broadcast.value
        if lat and lon:
            df = df.withColumn(
                "lat",
                when(df['lat'].isNull(), lat).otherwise(df['lat'])
            )
            df = df.withColumn(
                "lng",
                when(df['lng'].isNull(), lon).otherwise(df['lng'])
            )
    return df


restaurant_df = fill_missing_lat_lon(restaurant_df)

# Generate geohash and add it as a new column
restaurant_df = restaurant_df.withColumn("geohash", geohash_udf("lat", "lng"))

weather_df = weather_df.withColumnRenamed("lat", "weather_lat").withColumnRenamed("lng", "weather_lng")
# Generate geohash for weather data
weather_df = weather_df.withColumn("geohash", geohash_udf("weather_lat", "weather_lng"))

# Perform a left join between restaurant and weather data on geohash assuming weather data has a 'geohash' column to join on
enriched_df = restaurant_df.join(weather_df, "geohash", "left")

# Remove nulls and duplicates after the join
enriched_df = enriched_df.dropna().dropDuplicates()

# Save the enriched data to Parquet
output_path = "output/enriched_data.parquet"
enriched_df.write.parquet(output_path, mode="overwrite", partitionBy=["geohash"])

enriched_df.show(5)
