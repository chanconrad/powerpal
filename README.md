The script reads Powerpal data into InfluxDB. Checks the timestamp of the latest data point in Influx, and then requests the newer points from Powepal.

Set the Powerpal serial number and token:
```
export POWERPAL_SERIAL="XXXXXXXX"
export POWEPAL_TOKEN="XXXXXXXXXX"
```

Set the InfluxDB details:
```
INFLUX_URL="http://localhost:8086"
INFLUX_TOKEN="XXXXXXXXXX"
INFLUX_ORG="org-name"
INFLUX_BUCKET="bucket-name"
INFLUX_MEASUREMENT="measurement-name"
```

Run the script:
```
python powerpal.py
```
