import RPi.GPIO as GPIO
import time

# Use the BCM pin numbering scheme
GPIO.setmode(GPIO.BCM)

# Set the LED pin number (replace 18 with your actual pin number)
LED_PIN = 4

# Set the LED pin as output
GPIO.setup(LED_PIN, GPIO.OUT)

try:
    # Flash the LED indefinitely
    while True:
        # Turn on the LED
        GPIO.output(LED_PIN, GPIO.HIGH)
        time.sleep(1)  # Change this value to adjust the on time

        # Turn off the LED
        GPIO.output(LED_PIN, GPIO.LOW)
        time.sleep(1)  # Change this value to adjust the off time

except KeyboardInterrupt:
    # Clean up GPIO when exiting
    GPIO.cleanup()
    print("Exiting...")