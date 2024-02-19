import json
import os
from datetime import datetime
from time import sleep

stop = False
from LCD import LCD

lcd = LCD(2, 0x27, True)  # params available for rPi revision, I2C Address, and backlight on/off
# lcd = LCD(2, 0x3F, True)

lcd.message("Staring Session!", 1)  # display 'Hello World!' on line 1 of LCD

sleep(2)  # wait 1 seconds
LCD(2, 0x27, False)
sleep(0.5)
LCD(2, 0x27, True)
lcd.clear()  # clear LCD


def timenow():
    lcd.message(datetime.now().strftime("%H:%M:%S :D PiOK"), 1)


def ourip():
    ip = os.popen('hostname -I').read()
    lcd.message(ip, 1)


def displayloop():
    global stop
    sleep(3)
    while True:
        try:
            timenow()
            # Open Current.json
            try:
                with open("current.json", "r") as current_json:
                    current = json.load(current_json)
                timenow()
            except:
                print("JSON Read Fail")
                lcd.message("JSON Read Fail", 2)
                sleep(2)
            try:
                timenow()
                # Show UV Value
                lcd.message(f"UV Index: {int(current['UV'])} %", 2)
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                # Show Light Value
                lcd.message(f"Light: {int(current['light'])} %", 2)
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                # Show Temperature
                lcd.message(f"Temp: {current['temp']} C", 2)
                ourip()
                sleep(4)
                # Show Humidity
                lcd.message(f"Humidity: {current['air_humidity']} %", 2)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                # Show Soil Humidity
                lcd.message(f"Soil Humid: {int(current['soil_humidity'])} %", 2)
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                timenow()
                sleep(1)
                timenow()
            except:
                print("Decoding failed")
                lcd.message("Err Decoding JSON", 2)
                sleep(2)
        except KeyboardInterrupt or stop or SystemExit:
            lcd.clear()
            lcd.message("Shutting Down!", 1)
            sleep(1)
            lcd.clear()
            LCD(2, 0x27, False)
            print("Quitting !")
            exit()


def stopdisplay():
    lcd = LCD(2, 0x27, False)
    lcd.clear()
    exit()


# MAIN RUN HERE
# displayloop()

