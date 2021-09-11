"""

OUTPUT FORMAT:
<time>,<device-id>,<region>,<pressure-value>

"""

import socket
import sys
import time
import random
from datetime import datetime

MEAN_WEIGHT = 75
VARIANCE = 20
if len(sys.argv) < 5:
    print("Format: <host> <port> <device-id> <region> [<pressure-mean>] [<pressure-std>]", sys.argv[0])
    exit(-1)

HOST = sys.argv[1]
PORT = int(sys.argv[2])
DEVICE_ID = sys.argv[3]
REGION = sys.argv[4]
MEAN_WEIGHT = sys.argv[5] if len(sys.argv) >= 6 else MEAN_WEIGHT
WEIGHT_VARIANCE = sys.argv[6] if len(sys.argv) >= 7 else VARIANCE

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
                    weight_measured = random.gauss(MEAN_WEIGHT, WEIGHT_VARIANCE)
                    data = "{},{},{},{:.2f}".format(int(time.time()) * 1000, DEVICE_ID, REGION, weight_measured)
                    print(data+"\n")
                    conn.sendall("{}\n".format(data).encode('utf-8'))
                    time.sleep(1)
                except Exception as e:
                    print(e)
                    break
