import threading
from datetime import datetime
import uvicorn
import serialreader
import dht11reader
from time import sleep
import i2cdisplay
# from gpiozero import LED
import json
print("Importing finished")
import relaycontroller
print("Relay controller imported")
from model.DataManager import DataManager, Data, DataIn
print("Import finished")
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

threading.Thread(target=serialreader.readloop).start()
# print("Serial reader started")
threading.Thread(target=dht11reader.dht11reader).start()
# print("DHT11 reader started")
threading.Thread(target=lambda: uvicorn.run("fastapihandler:app", host="0.0.0.0")).start()
# print("FastAPI started")
threading.Thread(target=relaycontroller.relaycontroller).start()
# print("Relay controller started")
threading.Thread(target=i2cdisplay.displayloop).start()
sleep(2)
data_man = DataManager()
while True :
    try :
        currentjson = open("current.json", "w")
        print(f"Dirt: {serialreader.dirt} UV: {serialreader.uv} Light: {serialreader.light}")
        print(f"Temp: {dht11reader.temperature_c} C    Humidity: {dht11reader.humidity}")
        data = DataIn(
            data_time=datetime.now() ,
            UV=serialreader.uv if serialreader.uv is not None else -1.0,
            light=serialreader.light if serialreader.light is not None else -1.0,
            temp=dht11reader.temperature_c if dht11reader.temperature_c is not None else -1.0,
            air_humidity=dht11reader.humidity if dht11reader.humidity is not None else -1.0,
            soil_humidity=serialreader.dirt if serialreader.dirt is not None else -1.0
        )
        data_man.insert(data)
        print(data.__dict__)
        currentjson.write(json.dumps(data.__dict__, cls=DateTimeEncoder))
        currentjson.close()
        sleep(30)
    except KeyboardInterrupt or SystemExit or serialreader.stop or dht11reader.stop:
        # Stop all active thread
        serialreader.stop = True
        # i2cdisplay.stop = True
        dht11reader.stop = True
        # i2cdisplay.stopdisplay()
        print("Quitting !")
        break