import os
from datetime import datetime

import requests
from dateutil.tz import tzlocal
from influxdb_client import InfluxDBClient, Point

POWERPAL_URL = "https://readings.powerpal.net"
POWERPAL_SERIAL = os.getenv("POWERPAL_SERIAL")
POWERPAL_TOKEN = os.environ.get("POWERPAL_TOKEN")

POWERPAL_DEVICE_PATH = "/api/v1/device/"
POWERPAL_READING_PATH = "/api/v1/meter_reading/"

# Data is available in 60 second intervals
POWERPAL_INTERVAL = 60
POWERPAL_BATCH_SIZE = 50000

POWERPAL_FIELDS = {"watt_hours": "Wh"}

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
INFLUX_MEASUREMENT = os.getenv("INFLUX_MEASUREMENT")

device_url = POWERPAL_URL + POWERPAL_DEVICE_PATH + POWERPAL_SERIAL


def reading_url(start, end):
    return (
        POWERPAL_URL
        + POWERPAL_READING_PATH
        + POWERPAL_SERIAL
        + f"?start={start}&end={end}"
    )


def powerpal_query(url):
    # Request headers
    headers = {
        "Accept": "application/json",
        "Accept-Language": "en-au",
        "Authorization": POWERPAL_TOKEN,
        "User-Agent": "Powerpal/1895 CFNetwork/1240.0.4 Darwin/20.5.0",
    }

    result = session.get(url, headers=headers)

    if result.status_code != 200:
        print("Request failed.")
        exit()
    else:
        return result.json()


def read_powerpal_usage(batch_start, batch_end):
    request_url = reading_url(batch_start, batch_end)
    return powerpal_query(request_url)


# Set up connection to influx
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()
query_api = client.query_api()

# Create session
session = requests.session()

# Make request
data = powerpal_query(device_url)

# Get starting value from powerpal
start = data["first_reading_timestamp"]

# Get latest timestamp from influx
query = f'from(bucket:"{INFLUX_BUCKET}")\
  |> range(start: -100d) \
  |> filter(fn:(r) => r._measurement == "{INFLUX_MEASUREMENT}") \
  |> group() \
  |> last() '
result = query_api.query(org=INFLUX_ORG, query=query)
if len(result) == 1:
    latest_influx_timestamp = datetime.timestamp(result[0].records[0]["_time"])

    # Only read from the latest timestamp from powerpal
    if latest_influx_timestamp > start:
        start = latest_influx_timestamp + 1

# Get data points from powerpal
nbatch = 0
while start < data["last_reading_timestamp"]:
    nbatch += 1

    batch_start = start
    batch_end = start + POWERPAL_BATCH_SIZE * POWERPAL_INTERVAL

    # Increment next point by 1 second to avoid reading the same point twice
    start = batch_end + 1

    # Read batch from powerpal
    data_points = read_powerpal_usage(batch_start, batch_end)

    print(f"Reading batch {nbatch}: {len(data_points)} points")

    # Write points to influx
    for point in data_points:
        t = datetime.fromtimestamp(point["timestamp"], tz=tzlocal())

        for key in POWERPAL_FIELDS:
            p = (
                Point(INFLUX_MEASUREMENT)
                .field(key, float(point[key]))
                .tag("units", POWERPAL_FIELDS[key])
                .time(t)
            )
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)

# Flush writes
write_api.close()
