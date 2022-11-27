from ruuvitag_sensor.ruuvi import RuuviTagSensor
from datetime import datetime

from config import UPDATE_TIMEOUT

    
class RuuviSniffer:
    def __init__(self) -> None:
        self.latest_update: datetime = datetime.now()
        self.data: dict = {}

    def start(self):
        RuuviTagSensor.get_data(self.handle_data)

    def handle_data(self, bluetooth_data):
        print(f"Got data: {bluetooth_data}")
        mac = bluetooth_data[0]
        sensor_data = bluetooth_data[1]
        self.data[mac] = sensor_data

        if (datetime.now() - self.latest_update).seconds > UPDATE_TIMEOUT:
            self.upload_data()

    def upload_data(self):
        print(f"Uploading data..")
        self.latest_update = datetime.now()
        self.data = {}

    
if __name__ == '__main__':
    ruuvi = RuuviSniffer()
    ruuvi.start()