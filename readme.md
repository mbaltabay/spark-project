# Restaurant Data Enrichment and Weather Join

This project implements the enrichment of restaurant data by filling in missing latitude and longitude values using the OpenCage Geocoding API. Additionally, it performs a join between restaurant data and weather data using geohashes, and stores the enriched data in a partitioned Parquet format.

## Features
- **Enrich missing geolocation data**: Missing latitude and longitude values for restaurants are fetched using the OpenCage Geocoding API.
- **Geohashing**: Geohash values (4 characters long) are generated using the restaurant latitude and longitude for efficient geographic joins.
- **Data Joining**: Weather data and restaurant data are left-joined on the geohash, avoiding data duplication.
- **Storage**: The enriched data is stored in Parquet format, preserving partitioning.
- **API Rate Limiting Handling**: The OpenCage API key is rate-limited, and delays are added to manage API calls.

## Requirements
- Python 3.8+
- PySpark
- `requests` for API calls
- `geohash` for generating geohashes
- `unittest` or `pytest` for testing

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/mbaltabay/spark-project.git
   cd spark_project
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up your **OpenCage API Key**:
   - Obtain an API key from [OpenCage Geocoding API](https://opencagedata.com/).
   - Set the key as an environment variable:
     ```bash
     export OPENCAGE_API_KEY="your-api-key"
     ```

## Usage

### 1. **Enrich Restaurant Data with Latitude and Longitude**

Run the script to fill missing geolocation data in the restaurant dataset using the OpenCage API:

```bash
python main.py
```

This will:
- Read the `part-0.csv` restaurant data file.
- Call the OpenCage API to fetch missing latitude and longitude values.
- Generate a 4-character geohash for each restaurant based on latitude and longitude.
- Join restaurant data with weather data based on the geohash.
- Write the enriched data to a Parquet file (`output_data.parquet`).

### 2. **Testing**

Unit tests are included for the key functionality of the system, such as geocoding, geohashing, and data joining.

To run the tests:

#### Using `pytest`:
```bash
pytest test_main.py
```

### 3. **API Rate Limiting Handling**

To manage API rate limits:
- A delay is added between API calls to ensure that the rate limit is not exceeded.
- You can adjust the delay duration in the code.

## File Structure

- `enrich_restaurant_data.py`: Main script for processing and enriching the restaurant data.
- `tests/`: Folder containing unit tests for the core functionality.
- `part-0.csv`: Input restaurant data (CSV).
- `weather_data.parquet`: Input weather data (Parquet).
- `output_data.parquet`: Output enriched data (Parquet).

## Example Output

The script will output the enriched data as a partitioned Parquet file. The output will contain the following columns:
- `restaurant_id`: Unique identifier for the restaurant.
- `restaurant_name`: Restaurant name.
- `lat`: Latitude of the restaurant.
- `lng`: Longitude of the restaurant.
- `geohash`: 4-character geohash of the restaurant location.
- `avg_tmpr_c`: Average temperature in Celsius (from weather data).
- `avg_tmpr_f`: Average temperature in Fahrenheit (from weather data).
- `wthr_date`: Weather data date (from weather data).

## Notes

- Make sure to respect the OpenCage API rate limits by adding appropriate delays.
- This solution assumes that the input data (`part-0.csv` for restaurants and `weather_data.parquet`) is correctly formatted and available.
- The project uses PySpark for distributed data processing.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author
Mereke Baltabay