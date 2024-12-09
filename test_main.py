import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from main import get_lat_lon_from_opencage, fill_missing_lat_lon, generate_geohash
import time

# Mocking the OpenCage API function
@patch('main.get_lat_lon_from_opencage')
def test_fill_missing_lat_lon(mock_get_lat_lon_from_opencage):
    # Mock the return value for the API call
    time.sleep(5)
    mock_get_lat_lon_from_opencage.return_value = (40.748817, -73.985428)  # Example coordinates for New York

    # Create a Spark session for testing
    spark = SparkSession.builder \
        .appName("Test Restaurant Data") \
        .master("local[*]") \
        .getOrCreate()

    # Example restaurant dataframe (with some missing latitudes/longitudes)
    data = [
        ("Restaurant 1", None, None, "New York"),
        ("Restaurant 2", 40.712776, -74.005974, "New York"),
    ]
    columns = ["franchise_name", "lat", "lng", "city"]
    restaurant_df = spark.createDataFrame(data, columns)

    # Apply the function to fill missing latitudes and longitudes
    restaurant_df = fill_missing_lat_lon(restaurant_df)

    # Collect the results
    result = restaurant_df.collect()

    # Check that the missing lat/lon values were filled correctly
    assert result[0]["lat"] == 40.748817
    assert result[0]["lng"] == -73.985428
    assert result[1]["lat"] == 40.712776
    assert result[1]["lng"] == -74.005974


@patch('main.get_lat_lon_from_opencage')
def test_generate_geohash(mock_get_lat_lon_from_opencage):
    time.sleep(5)
    # Mock the return value for the API call (no longer needed here, but included for completeness)
    mock_get_lat_lon_from_opencage.return_value = (40.748817, -73.985428)  # Example coordinates

    # Test the geohash generation function
    lat = 40.748817
    lon = -73.985428
    geohash_result = generate_geohash(lat, lon)

    # Check that the geohash is generated correctly (4 characters in length)
    assert geohash_result == "dr5ruv"


def test_generate_geohash_empty():
    time.sleep(5)
    # Test the geohash generation with None values (invalid lat/lon)
    lat = None
    lon = None
    geohash_result = generate_geohash(lat, lon)

    # Check that the geohash result is None for invalid lat/lon
    assert geohash_result is None
