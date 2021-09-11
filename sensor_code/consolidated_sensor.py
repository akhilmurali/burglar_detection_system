"""

OUTPUT FORMAT:
<time>,<mw_device-id>,<mw_region>,<doppler-frequency-value>,<pressure_sensor_device-id>,<pressure_sensor_region>,<pressure-value>,<ir_fence_device-id>,<ir_fence_region>,<ir-fence-value>

"""

import socket
import sys
import time
import random
from datetime import datetime

MEAN_FREQENCY = 120
VARIANCE = 40
MEAN_WEIGHT = 75
WEIGHT_VARIANCE = 30
if len(sys.argv) < 5:
    print("Format: <host> <port> <device-id> <region> [<doppler-frequency-mean>] [<doppler-frequency-std>] [<pressure-mean>] [pressure-std]", sys.argv[0])
    exit(-1)

HOST = sys.argv[1]
PORT = int(sys.argv[2])
DEVICE_ID = sys.argv[3]
REGION = sys.argv[4]
DOPPLER_MEAN = sys.argv[5] if len(sys.argv) >= 6 else MEAN_FREQENCY
DOPPLER_VAR = sys.argv[6] if len(sys.argv) >= 7 else VARIANCE
PRESSURE_MEAN = sys.argv[7] if len(sys.argv) >= 8 else MEAN_WEIGHT
PRESSURE_VAR = sys.argv[8] if len(sys.argv) >= 9 else WEIGHT_VARIANCE
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(1)
    while True:
        conn, addr = s.accept()
        print("Connected to: {}".format(addr))
        with conn:
            while True:
                try:
                    doppler_frequency = random.gauss(DOPPLER_MEAN, DOPPLER_VAR)
                    pressure = random.gauss(PRESSURE_MEAN, PRESSURE_VAR)
                    ir_sensor =  random.randint(0, 255)
                    data = "{},{},{},{:.2f},{},{},{:.2f},{},{},{}".format(int(time.time())*1000,f'mw{DEVICE_ID}',f'mw{REGION}',doppler_frequency,f'p{DEVICE_ID}',f'p{REGION}',pressure,f'ir{DEVICE_ID}',f'ir{REGION}',ir_sensor)
                    print(data+"\n")
                    conn.sendall("{}\n".format(data).encode('utf-8'))
                    time.sleep(1)
                except Exception as e:
                    print(e)
                    break
