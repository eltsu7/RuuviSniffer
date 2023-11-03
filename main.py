import logging
from datetime import datetime
import sys

from ruuvitag_sensor.ruuvi import RuuviTagSensor
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

from config import (
    UPDATE_TIMEOUT,
    INFLUX_TOKEN,
    INFLUX_HOST,
    INFLUX_ORG,
    INFLUX_BUCKET,
    SENSORS,
)


log = logging.Logger("ruuvisniffer")
log.addHandler(logging.StreamHandler(sys.stdout))


class RuuviSniffer:
    def __init__(self) -> None:
        log.info("Initializing RuuviSniffer")
        self.latest_update: datetime = datetime.now()
        self.data: dict = {}
        self.database = InfluxDBClient(
            url=INFLUX_HOST, token=INFLUX_TOKEN, org=INFLUX_ORG
        ).write_api(write_options=SYNCHRONOUS)

    def start(self):
        log.info("Starting to scan")
        RuuviTagSensor.get_data(self.handle_data)

    def handle_data(self, bluetooth_data):
        mac = bluetooth_data[0].replace(":", "")

        # Don't do anything to not specified sensors
        if mac not in SENSORS.keys():
            return

        sensor_data = bluetooth_data[1]
        self.data[mac] = sensor_data

        if (datetime.now() - self.latest_update).seconds > UPDATE_TIMEOUT:
            self.upload_data()

    def upload_data(self):
        log.info("Uploading data")
        for mac, data in self.data.items():
            point = Point("ruuvi_measurements")
            for measurement in data:
                point.field(field=measurement, value=data[measurement])

            point.tag(key="mac", value=mac)
            point.tag(key="name", value=SENSORS[mac])

            try:
                self.database.write(INFLUX_BUCKET, record=point)
            except InfluxDBError as e:
                log.info(
                    f"InfluxDBError: Error sending data from {mac} to {INFLUX_BUCKET}"
                )
                log.info(f"Error object: {e.__dict__}")
            except Exception as e:
                log.info(f"Error sending data from {mac} to {INFLUX_BUCKET}")
                log.info(f"Error object: {e.__dict__}")

        log.info(f"{datetime.now()} - Sent data from: {', '.join(self.data.keys())}")
        self.data = {}
        self.latest_update = datetime.now()


if __name__ == "__main__":
    ruuvi = RuuviSniffer()
    ruuvi.start()
