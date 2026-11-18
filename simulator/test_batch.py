"""test_batch.py - which batch sizes survive ThingsBoard Cloud rate limits?"""
import json, os, time
from buffer import Buffer
from mqtt_client import TBClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tok = json.load(open(os.path.join(ROOT, "tokens.json")))["node1"]
c = TBClient(tok, Buffer("t.db"))

def connect():
    while not c.connected.is_set():
        c.next_try = 0
        c.try_connect()
        time.sleep(1)

base = int(time.time() * 1000) - 3_600_000     # one hour in the past
for n in [5, 10, 14, 15, 20, 50]:
    connect()
    rows = [{"id": i, "ts_ms": base + i * 1000, "ph": 7.1, "turbidity_ntu": 5.0,
             "tds_ppm": 200, "water_temp_c": 18.0, "flow_lpm": 12.0,
             "lat": 27.9, "lon": 85.1} for i in range(n)]
    base += 100_000
    ok = c._publish_wait([c._to_msg(r) for r in rows])
    print(f"batch of {n:3d} rows = {n*7:3d} data points -> "
          f"{'OK' if ok else 'FAILED (connection dropped)'}")
    time.sleep(3)