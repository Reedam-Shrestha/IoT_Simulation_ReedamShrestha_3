"""
mqtt_client.py - MQTT over TLS (8883) to ThingsBoard with QoS 1,
exponential backoff with jitter, and batched replay from SQLite.

Live and replayed messages both use the client-stamped form:
  {"ts": <epoch ms>, "values": {...}}
Replay uses the array form: [{"ts":..,"values":{..}}, ...]
"""
import json
import random
import ssl
import threading
import time

import paho.mqtt.client as mqtt
HOST = "mqtt.eu.thingsboard.cloud"
PORT = 8883
TOPIC = "v1/devices/me/telemetry"

BACKOFF_MIN = 2      # s: fast recovery after a short blip
BACKOFF_MAX = 60     # s: caps radio wake-ups during a long outage
PUBLISH_TIMEOUT = 10  # s to wait for PUBACK

RC_BAD_AUTH = (4, 5)  # bad credentials / not authorised (MQTT 3.1.1)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class TBClient:
    def __init__(self, token, buffer):
        self.token = token
        self.buf = buffer
        self.state = "OFFLINE"
        self.outage = False            # set by scenario/keyboard
        self.connected = threading.Event()
        self.auth_failed = False
        self.backoff = BACKOFF_MIN
        self.next_try = 0
        self._acks = {}                # mid -> threading.Event

        try:  # paho 2.x
            self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except AttributeError:  # paho 1.x
            self.c = mqtt.Client()
        self.c.username_pw_set(token, "")
        self.c.tls_set(cert_reqs=ssl.CERT_REQUIRED)  # system CA bundle
        self.c.on_connect = self._on_connect
        self.c.on_disconnect = self._on_disconnect
        self.c.on_publish = self._on_publish

    # ---------- callbacks ----------
    def _on_connect(self, c, ud, flags, rc):
        if rc == 0:
            self.connected.set()
            self.auth_failed = False
            self.backoff = BACKOFF_MIN
            log("[ONLINE] connected")
        elif rc in RC_BAD_AUTH:
            self.auth_failed = True
            log(f"[AUTH FAILURE] rc={rc}: check access token (not retrying fast)")
            self.c.disconnect()   # stop paho's internal auto-reconnect
        else:
            log(f"[NETWORK] connect refused rc={rc}")

    def _on_disconnect(self, c, ud, rc):
        self.connected.clear()
        if self.state != "OUTAGE" and not self.auth_failed:
            log(f"[NETWORK] disconnected rc={rc}")

    def _on_publish(self, c, ud, mid):
        ev = self._acks.get(mid)
        if ev:
            ev.set()

    # ---------- outage control ----------
    def start_outage(self):
        self.outage = True
        self.state = "OUTAGE"
        self.c.loop_stop()
        self.c.disconnect()
        self.connected.clear()
        log(f"[OUTAGE] link down, buffer={self.buf.depth()}")

    def end_outage(self):
        self.outage = False
        self.state = "RECONNECTING"
        self.next_try = 0
        log("[RECONNECTING] outage ended")

    # ---------- connection ----------
    def try_connect(self):
        """Call every tick. Non-blocking apart from the connect attempt."""
        if self.outage or self.connected.is_set():
            return
        now = time.time()
        if now < self.next_try:
            return
        try:
            self.c.connect(HOST, PORT, keepalive=60)
            self.c.loop_start()
            self.connected.wait(timeout=8)
        except (OSError, ssl.SSLError) as e:
            log(f"[NETWORK] connect failed: {e}")
        if not self.connected.is_set():
            self.c.loop_stop()
            wait = 600 if self.auth_failed else self.backoff
            delay = wait * random.uniform(0.5, 1.0)   # jitter
            self.next_try = time.time() + delay
            log(f"[RECONNECTING] retry in {delay:.1f}s (backoff {self.backoff}s)")
            self.backoff = min(self.backoff * 2, BACKOFF_MAX)

    # ---------- publish ----------
    def _publish_wait(self, payload):
        """Publish with QoS 1 and block until PUBACK. True only if confirmed."""
        info = self.c.publish(TOPIC, json.dumps(payload), qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return False
        ev = self._acks.setdefault(info.mid, threading.Event())
        ok = ev.wait(timeout=PUBLISH_TIMEOUT)
        self._acks.pop(info.mid, None)
        return ok

    @staticmethod
    def _to_msg(row):
        return {"ts": row["ts_ms"], "values": {
            "ph": row["ph"], "turbidity_ntu": row["turbidity_ntu"],
            "tds_ppm": row["tds_ppm"], "water_temp_c": row["water_temp_c"],
            "flow_lpm": row["flow_lpm"],
            "latitude": row["lat"], "longitude": row["lon"]}}

    def flush(self, batch_size=50):
        """Drain unsynced rows oldest-first. Stops if a publish fails."""
        if not self.connected.is_set():
            return 0
        total = self.buf.depth()
        sent = 0
        t0 = time.time()
        while self.connected.is_set():
            rows = self.buf.next_batch(batch_size)
            if not rows:
                break
            if total > batch_size:
                self.state = "FLUSHING"
                log(f"[FLUSHING {sent}/{total}]")
            if self._publish_wait([self._to_msg(r) for r in rows]):
                self.buf.mark_synced([r["id"] for r in rows])
                sent += len(rows)
            else:
                log("[NETWORK] batch not confirmed, will retry")
                break
        if sent:
            dt = max(time.time() - t0, 1e-6)
            log(f"[FLUSHED] {sent} rows in {dt:.2f}s = {sent/dt:.1f} rows/s")
        if self.connected.is_set():
            self.state = "ONLINE"
        return sent