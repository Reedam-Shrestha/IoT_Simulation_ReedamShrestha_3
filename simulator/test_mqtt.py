"""test_mqtt.py - writes 5 past-dated rows, flushes them, checks timestamps."""
import json
import os
import time

from buffer import Buffer
from mqtt_client import TBClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tok = json.load(open(os.path.join(ROOT, "tokens.json")))["node1"]

b = Buffer("t.db")
v = {"ph": 7.1, "turbidity_ntu": 5.0, "tds_ppm": 200, "water_temp_c": 18.0,
     "flow_lpm": 12.0, "latitude": 27.9, "longitude": 85.1}
now = int(time.time() * 1000)
for k in range(5):                    # 5 rows, 1 minute apart, in the past
    b.add(now - (5 - k) * 60000, v)

c = TBClient(tok, b)
while not c.connected.is_set():
    c.try_connect()
    time.sleep(1)
c.flush(batch_size=50)
print("depth after flush:", b.depth())