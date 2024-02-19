import json
import gpiozero
import time
print("Hi from RelayController")

# Set the GPIO pin numbers
UV_LAMP_PIN = 17
PUMP_PIN = 27

# Set the GPIO pin mode
uvrelay = gpiozero.OutputDevice(UV_LAMP_PIN, active_high=False, initial_value=False)
pumprelay = gpiozero.OutputDevice(PUMP_PIN, active_high=False, initial_value=False)

def read_json():
    with open("./current.json", "r") as current_json:
        current = json.load(current_json)
    with open("./userstate.json", "r") as state_json:
        state = json.load(state_json)
        print(state)
    return current['light'], current['soil_humidity'], state['uvlamp'], state['pump']

def relaycontroller():
    light = 60
    soil_humidity = 60
    uvlamp = "auto"
    pump = "auto"
    print("Relay Controller Started")
    while True:
        print("Relay Controller Running")
        try :
            light, soil_humidity, uvlamp, pump = read_json()
            print(light, soil_humidity, uvlamp, pump)
        except :
            print("Error reading JSON")
        try:
            if uvlamp == "auto":
                if light < 40:
                    uvstate="autoon"
                    uvrelay.on()
                else:
                    uvstate="autooff"
                    uvrelay.off()
            elif uvlamp == "on":
                uvstate="manon"
                uvrelay.on()
            else:
                uvstate="manoff"
                uvrelay.off()

            if pump == "auto":
                if soil_humidity < 50:
                    pumpstate="autoon"
                    pumprelay.on()
                else:
                    pumpstate="autooff"
                    pumprelay.off()
            elif pump == "on":
                pumpstate="manon"
                pumprelay.on()
            else:
                pumpstate="manoff"
                pumprelay.off()

            # Save to state.json
            state = {"pump": pumpstate, "uvlamp": uvstate}
            with open('state.json', 'w') as f:
                json.dump(state, f)
        except KeyboardInterrupt or SystemExit:
            print("Quitting !")
            break
        time.sleep(1)