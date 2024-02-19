from machine import ADC, Pin
from time import sleep

uv_pin = 12
light_pin = 13
soil_pin = 14


# Declare All The Fuction for Initialization
def uv_init():
    global uv
    uv = ADC(Pin(uv_pin))
    uv.atten(ADC.ATTN_11DB)


def light_init():
    global light
    light = ADC(Pin(light_pin))
    light.atten(ADC.ATTN_11DB)


def soil_init():
    global soil
    soil = ADC(Pin(soil_pin))
    soil.atten(ADC.ATTN_11DB)


# Define a function to read the sensor value
def read_all_sensor():
    sensor_value = uv.read(), light.read(), soil.read()
    return sensor_value


# Initialization
uv_init()
light_init()
soil_init()
# Main loop
while True:
    print(uv.read(), light.read(), soil.read())
    sleep(1)

