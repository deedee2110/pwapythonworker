# serialreader.py
import time

import serial

ser = serial.Serial('/dev/ttyUSB0', 115200)  # Change this to match your serial port

uv = None  # Define uv
dirt = None  # Define dirt
light = None

# Dirt mak = dry
# UV noi = bright
stop = False

def readloop():
    global uv, dirt, light  # Declare uv and dirt as global
    if not stop:
        while True:
            try:
                data = ser.readline().decode().strip()
                # print(data)
                rawuv, rawlight, rawdirt = (data.split(" "))  # update the shared variables
                uv = (int(rawuv)/4095)*100
                light = 100-((int(rawlight)/4095)*100)
                dirt = 100-(int(rawdirt)/4095)*100
            except:
                print("Error reading serial data.")
            time.sleep(1)
    elif KeyboardInterrupt or SystemExit or stop:
        ser.close()
        print("Serial connection closed.")
        exit()

# readloop()