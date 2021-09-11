"""
For Official Documentation and More Information Refer
https://docs.python.org/3/library/socket.html

OUTPUT FORMAT:
<time>,<device-id>,<region>,<fence_breach>
"""

import socket
import sys
import time
import random
from datetime import datetime

if len(sys.argv) < 5:
    print("Format: <host> <port> <beams-per-fence> <fences>", sys.argv[0])
    exit(-1)

HOST = sys.argv[1]
PORT = int(sys.argv[2])
BEAMS_PER_FENCE = sys.argv[3]
FENCES = sys.argv[4]
devices = []
regions = []
VALUE_ERROR_PROBABILITY = 0.01
MISSING_DATA_PROBABILITY = 0.01

for i in range(BEAMS_PER_FENCE):
    devices.append(f"device-{i}")

for j in range(FENCES):
    regions.append(f"region-{j}")

LIMIT =0.95 #Lower possibility of a perimeter breach

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
                    time.sleep(1)
                    for fence in FENCES:
                        fence_breach = random.random() > LIMIT 
                        for device_id in BEAMS_PER_FENCE:
                            # send empty data or incorrect data from time to time
                            if (random.random() < VALUE_ERROR_PROBABILITY):
                                fence_breach = "??"
                            if (random.random() < MISSING_DATA_PROBABILITY):
                                fence_breach = None
                            data = "{},{},{},{}".format(int(time.time()) * 1000, device_id, fence, fence_breach)
                            # data = "{},{},{},{}".format(datetime.now(), DEVICE_ID, REGION, fence_breach)
                            print(data)
                            conn.sendall("{}\n".format(data).encode('utf-8'))
                except Exception as e:
                    print(e)
                    break
