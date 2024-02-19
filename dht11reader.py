import time,board,adafruit_dht
stop = False
sensor = adafruit_dht.DHT11(board.D4)
temperature_c = None
humidity = None
def dht11reader():
    global temperature_c, humidity
    while True:
        if not stop:
            try:
                temperature_c = sensor.temperature
                humidity = sensor.humidity
                # print(f"Temp: {temperature_c} C    Humidity: {humidity}")
            except RuntimeError as error:
                print(error.args[0])
        elif KeyboardInterrupt or SystemExit or stop:
            print("DHT11 Sensor connection closed.")
            exit()
        time.sleep(2.0)