"""
main.py - runs the virtual edge nodes: generate -> SQLite -> MQTT/TLS.

Every reading is written to SQLite first. The network only drains the buffer.
Scenario events come from scenario.json; keys also work during a run:
  i = intrusion on selected node   1/2/3 = select node
  o = toggle outage (all nodes)    s = toggle supply cut (selected node)
  q = quit
"""
import argparse
import json
import os
import queue
import sys
import threading
import time

from generator import NodeGenerator
from buffer import Buffer
from mqtt_client import TBClient, log

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


class Tee:
    """Copy everything printed to a log file as well."""
    def __init__(self, *streams):
        self.streams = streams

    def write(self, s):
        for st in self.streams:
            st.write(s)
            st.flush()

    def flush(self):
        for st in self.streams:
            st.flush()


def start_keys(q):
    def run():
        try:
            import msvcrt
        except ImportError:                      # Linux/macOS: type key + Enter
            for line in sys.stdin:
                for ch in line.strip().lower():
                    q.put(ch)
            return
        while True:
            if msvcrt.kbhit():
                q.put(msvcrt.getwch().lower())
            time.sleep(0.05)
    threading.Thread(target=run, daemon=True).start()


class Node:
    def __init__(self, cfg, token, sc, data_dir, keep):
        self.name = cfg["name"]
        self.gen = NodeGenerator(cfg["name"], cfg["lat"], cfg["lon"], cfg.get("seed"))
        path = os.path.join(data_dir, self.name + ".db")
        if not keep:                              # fresh DB = repeatable demo
            for ext in ("", "-wal", "-shm"):
                if os.path.exists(path + ext):
                    os.remove(path + ext)
        self.buf = Buffer(path, sc["buffer_max_rows"])
        self.client = TBClient(token, self.buf)
        self.due = 0.0
        self.generated = 0
        self.evicted = 0
        self.burst = False


def in_burst(r):
    """Fast sampling while any value is beyond its alarm CLEAR level."""
    return (r["turbidity_ntu"] >= 40 or r["ph"] < 6.7 or r["ph"] > 8.3
            or r["tds_ppm"] >= 450)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", default=os.path.join(HERE, "scenario.json"))
    ap.add_argument("--speed", type=float, default=None)
    ap.add_argument("--keep", action="store_true", help="keep old SQLite files")
    args = ap.parse_args()

    sc = json.load(open(args.scenario))
    speed = args.speed or sc["speed"]
    base_iv, burst_iv = sc["baseline_interval_s"], sc["burst_interval_s"]
    batch = sc["batch_size"]
    default_peak = sc["default_peak_ntu"]

    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    logfile = open(os.path.join(
        ROOT, "logs", time.strftime("run_%Y%m%d_%H%M%S.log")), "w", encoding="utf-8")
    sys.stdout = Tee(sys.__stdout__, logfile)

    tokens = json.load(open(os.path.join(ROOT, "tokens.json")))
    data_dir = os.path.join(HERE, "data")
    os.makedirs(data_dir, exist_ok=True)

    nodes = {}
    for cfg in sc["nodes"]:
        tok = tokens.get(cfg["name"])
        if not tok or "PASTE" in tok:
            log(f"[SETUP] {cfg['name']} has no token in tokens.json - skipped")
            continue
        nodes[cfg["name"]] = Node(cfg, tok, sc, data_dir, args.keep)
    if not nodes:
        log("[SETUP] no nodes to run")
        return

    start = time.time()

    def phys(wall):          # compressed physics clock fed to the generator
        return start + (wall - start) * speed

    def targets(name):
        if name == "all":
            return list(nodes.values())
        return [nodes[name]] if name in nodes else []

    def apply_event(ev, wall):
        kind, who = ev["event"], ev["node"]
        for n in targets(who):
            if kind == "intrusion":
                peak = ev.get("peak_ntu", default_peak)
                n.gen.start_intrusion(phys(wall), peak_ntu=peak)
                log(f"[EVENT] intrusion on {n.name}, peak {peak} NTU")
            elif kind == "supply_cut_start":
                n.gen.set_supply_cut(True)
                log(f"[EVENT] supply cut on {n.name}")
            elif kind == "supply_cut_end":
                n.gen.set_supply_cut(False)
                log(f"[EVENT] supply restored on {n.name}")
            elif kind == "outage_start":
                n.client.start_outage()
            elif kind == "outage_end":
                n.client.end_outage()

    def step(n, now):
        made, last = 0, None
        while n.due <= now:                       # catch up if we were stalled
            t_acq = n.due
            r = n.gen.read(phys(t_acq))
            ev = n.buf.add(int(t_acq * 1000), r)  # SQLite FIRST
            n.generated += 1
            made += 1
            last = r
            if ev:
                n.evicted += ev
                log(f"[EVICT] {n.name} dropped {ev} oldest rows (cap reached)")
            n.burst = in_burst(r)
            n.due += burst_iv if n.burst else base_iv
        c = n.client
        c.try_connect()
        if c.connected.is_set():
            c.flush(batch)
        if made:
            if c.outage:
                label = "[OUTAGE]"
            elif not c.connected.is_set():
                label = "[OFFLINE]"
            else:
                label = "[LIVE]"
            log(f"{label} {n.name} ph={last['ph']:.2f} NTU={last['turbidity_ntu']:.1f} "
                f"TDS={last['tds_ppm']} T={last['water_temp_c']:.1f} "
                f"F={last['flow_lpm']:.1f} | buffer={n.buf.depth()}"
                f"{' BURST' if n.burst else ''}")

    timeline = sorted(sc["timeline"], key=lambda e: e["t"])
    ti = 0
    for n in nodes.values():
        n.due = start
    keys = queue.Queue()
    start_keys(keys)
    selected = next(iter(nodes))
    log(f"[START] nodes={list(nodes)} speed={speed}x keys: i o s 1 2 3 q")

    try:
        while True:
            now = time.time()
            elapsed = now - start
            if sc.get("duration_s") and elapsed >= sc["duration_s"]:
                log("[STOP] scenario duration reached")
                break
            while ti < len(timeline) and timeline[ti]["t"] <= elapsed:
                apply_event(timeline[ti], start + timeline[ti]["t"])
                ti += 1
            quit_now = False
            while not keys.empty():
                k = keys.get()
                if k == "q":
                    quit_now = True
                elif k in "123" and f"node{k}" in nodes:
                    selected = f"node{k}"
                    log(f"[KEY] selected {selected}")
                elif k == "i":
                    apply_event({"event": "intrusion", "node": selected}, time.time())
                elif k == "s":
                    n = nodes[selected]
                    on = not n.gen.supply_cut
                    apply_event({"event": "supply_cut_start" if on else "supply_cut_end",
                                 "node": selected}, time.time())
                elif k == "o":
                    down = any(n.client.outage for n in nodes.values())
                    apply_event({"event": "outage_end" if down else "outage_start",
                                 "node": "all"}, time.time())
            if quit_now:
                log("[STOP] quit key")
                break
            for n in nodes.values():
                step(n, now)
            time.sleep(0.2)
    except KeyboardInterrupt:
        log("[STOP] Ctrl+C")
    finally:
        for n in nodes.values():
            c = n.client
            if c.connected.is_set():
                c.flush(batch)
            synced = n.buf.db.execute(
                "SELECT COUNT(*) FROM telemetry_buffer WHERE synced=1").fetchone()[0]
            ratio = 100.0 * synced / n.generated if n.generated else 0.0
            log(f"[SUMMARY] {n.name} generated={n.generated} synced={synced} "
                f"unsynced={n.buf.depth()} evicted={n.evicted} delivery={ratio:.2f}%")
            c.c.loop_stop()
            c.c.disconnect()


if __name__ == "__main__":
    main()