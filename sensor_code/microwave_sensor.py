"""

OUTPUT FORMAT:
<time>,<device-id>,<region>,<doppler-frequency-value>

"""

import socket
import sys
import time
import random
from datetime import datetime

MEAN_FREQENCY = 120
VARIANCE = 40
if len(sys.argv) < 5:
    print("Format: <host> <port> <device-id> <region> [<doppler-frequency-mean>] [<doppler-frequency-std>] [<pressure-mean>] [pressure-std]", sys.argv[0])
    exit(-1)

HOST = sys.argv[1]
PORT = int(sys.argv[2])
DEVICE_ID = sys.argv[3]
REGION = sys.argv[4]
DOPPLER_MEAN = sys.argv[5] if len(sys.argv) >= 6 else MEAN_FREQENCY
DOPPLER_STD = sys.argv[6] if len(sys.argv) >= 7 else VARIANCE

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
                    doppler_frequency = random.gauss(DOPPLER_MEAN, DOPPLER_STD)
                    data = "{},{},{},{:.2f}".format(int(time.time()) * 1000, DEVICE_ID, REGION, doppler_frequency)
                    print(data+"\n")
                    conn.sendall("{}\n".format(data).encode('utf-8'))
                    time.sleep(0.1)
                except Exception as e:
                    print(e)
                    break
