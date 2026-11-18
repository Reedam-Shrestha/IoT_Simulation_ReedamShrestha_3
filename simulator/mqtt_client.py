"""
mqtt_client.py - MQTT over TLS (8883) to ThingsBoard Cloud with QoS 1,
exponential backoff with jitter, and rate-limited batched replay from SQLite.

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

HOST = "mqtt.eu.thingsboard.cloud"   # US region: mqtt.thingsboard.cloud
PORT = 8883
TOPIC = "v1/devices/me/telemetry"

BACKOFF_MIN = 2       # s: fast recovery after a short blip
BACKOFF_MAX = 60      # s: caps radio wake-ups during a long outage
AUTH_RETRY = 600      # s: a bad token will not fix itself, so retry rarely
PUBLISH_TIMEOUT = 10  # s to wait for PUBACK

# ThingsBoard Cloud free plan: 100 data points/s per device AND per tenant.
KEYS_PER_ROW = 7
DEVICE_DP_PER_S = 100
TENANT_DP_PER_S = 100
SAFETY = 0.7          # stay at 70% of the limit
PUB_GAP = 1.0         # s between publishes (limits are per second)

RC_BAD_AUTH = (4, 5)  # bad credentials / not authorised (MQTT 3.1.1)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class TBClient:
    def __init__(self, token, buffer):
        self.token = token
        self.buf = buffer
        self.state = "OFFLINE"
        self.outage = False            # set by scenario / keyboard
        self.connected = threading.Event()
        self.auth_failed = False
        self.backoff = BACKOFF_MIN
        self.next_try = 0
        self.next_pub = 0
        self._acks = {}                # mid -> threading.Event
        self.max_rows = 10             # rows per publish, see set_rate()
        self.budget = SAFETY * DEVICE_DP_PER_S
        self._fl_active = False        # a backlog flush is in progress
        self._fl_t0 = 0.0
        self._fl_sent = 0
        self._fl_total = 0

        try:  # paho 2.x
            self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except AttributeError:  # paho 1.x
            self.c = mqtt.Client()
        self.c.username_pw_set(token, "")
        self.c.tls_set(cert_reqs=ssl.CERT_REQUIRED)  # system CA bundle
        self.c.on_connect = self._on_connect
        self.c.on_disconnect = self._on_disconnect
        self.c.on_publish = self._on_publish

    # ---------- rate limit ----------
    def set_rate(self, n_nodes=1):
        """Cap rows per publish so n_nodes together stay under the platform limit."""
        budget = SAFETY * min(DEVICE_DP_PER_S, TENANT_DP_PER_S / max(1, n_nodes))
        self.budget = budget
        self.max_rows = max(1, int(budget // KEYS_PER_ROW))
        log(f"[RATE] {n_nodes} node(s): max {self.max_rows} rows per publish, "
            f"{budget:.0f} data points/s")

    # ---------- callbacks ----------
    def _on_connect(self, c, ud, flags, rc):
        if rc == 0:
            self.auth_failed = False
            self.state = "ONLINE"
            self.connected.set()
            log("[ONLINE] connected")
        elif rc in RC_BAD_AUTH:
            self.auth_failed = True
            log(f"[AUTH FAILURE] rc={rc}: check access token (not retrying fast)")
            self.c.disconnect()   # we own reconnection, not paho
        else:
            log(f"[NETWORK] connect refused rc={rc}")

    def _on_disconnect(self, c, ud, rc):
        self.connected.clear()
        if self.outage or self.auth_failed:
            return
        if rc != 0:
            log(f"[NETWORK] connection lost rc={rc}")
            self._schedule_retry()
            self.c.disconnect()   # stop paho's own auto-reconnect

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
        self._fl_active = False
        log(f"[OUTAGE] link down, buffer={self.buf.depth()}")

    def end_outage(self):
        self.outage = False
        self.state = "RECONNECTING"
        self.next_try = 0
        self.backoff = BACKOFF_MIN
        log("[RECONNECTING] outage ended")

    # ---------- connection ----------
    def _schedule_retry(self):
        wait = AUTH_RETRY if self.auth_failed else self.backoff
        delay = wait * random.uniform(0.5, 1.0)       # jitter
        self.next_try = time.time() + delay
        kind = "auth" if self.auth_failed else "network"
        log(f"[RECONNECTING] {kind} retry in {delay:.1f}s (backoff {self.backoff}s)")
        self.backoff = min(self.backoff * 2, BACKOFF_MAX)

    def try_connect(self):
        """Call every tick. Returns quickly unless a connect attempt is due."""
        if self.outage or self.connected.is_set():
            return
        if time.time() < self.next_try:
            return
        self.c.loop_stop()                            # clean slate
        try:
            self.c.connect(HOST, PORT, keepalive=60)
            self.c.loop_start()
            self.connected.wait(timeout=8)
        except (OSError, ssl.SSLError) as e:
            log(f"[NETWORK] connect failed: {e}")
        if not self.connected.is_set() and time.time() >= self.next_try:
            self._schedule_retry()

    # ---------- publish ----------
    def _publish_wait(self, payload):
        """Publish with QoS 1 and wait for PUBACK. True only if confirmed."""
        info = self.c.publish(TOPIC, json.dumps(payload), qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return False
        ev = self._acks.setdefault(info.mid, threading.Event())
        deadline = time.time() + PUBLISH_TIMEOUT
        ok = False
        while time.time() < deadline:
            if ev.wait(0.2):
                ok = True
                break
            if not self.connected.is_set():           # link died mid-publish
                break
        self._acks.pop(info.mid, None)
        return ok

    @staticmethod
    def _to_msg(row):
        return {"ts": row["ts_ms"], "values": {
            "ph": row["ph"], "turbidity_ntu": row["turbidity_ntu"],
            "tds_ppm": row["tds_ppm"], "water_temp_c": row["water_temp_c"],
            "flow_lpm": row["flow_lpm"],
            "latitude": row["lat"], "longitude": row["lon"]}}

    def flush(self, batch_size=50, drain=False):
        """Send unsynced rows oldest-first, one rate-limited batch per call
        (drain=True keeps going until empty, used at shutdown)."""
        if not self.connected.is_set():
            return 0
        n = max(1, min(batch_size, self.max_rows))
        sent_total = 0
        while self.connected.is_set():
            now = time.time()
            if now < self.next_pub:
                if not drain:
                    break
                time.sleep(self.next_pub - now)
            rows = self.buf.next_batch(n)
            if not rows:
                break
            if not self._fl_active:
                self._fl_active = True
                self._fl_t0 = time.time()
                self._fl_sent = 0
                self._fl_total = self.buf.depth()
                self._fl_total = max(self._fl_total, self._fl_sent + self.buf.depth())
            if self._fl_total > 1:
                self.state = "FLUSHING"
                log(f"[FLUSHING {self._fl_sent}/{self._fl_total}] batch={len(rows)}")
            ok = self._publish_wait([self._to_msg(r) for r in rows])
            self.next_pub = time.time() + len(rows) * KEYS_PER_ROW / self.budget
            if ok:
                self.buf.mark_synced([r["id"] for r in rows])
                self._fl_sent += len(rows)
                sent_total += len(rows)
                self.backoff = BACKOFF_MIN            # delivery proven
            else:
                self.max_rows = max(1, self.max_rows // 2)
                n = max(1, min(n, self.max_rows))
                self.next_pub = time.time() + 5
                log(f"[NETWORK] batch not confirmed; max batch now {self.max_rows}")
                break
            if not drain:
                break
        if self._fl_active and self.buf.depth() == 0:
            if self._fl_sent > 1:
                dt = max(time.time() - self._fl_t0, 1e-6)
                log(f"[FLUSHED] {self._fl_sent} rows in {dt:.1f}s = "
                    f"{self._fl_sent/dt:.1f} rows/s, buffer empty")
            self._fl_active = False
        if self.connected.is_set() and self.state == "FLUSHING" and self.buf.depth() == 0:
            self.state = "ONLINE"
        return sent_total