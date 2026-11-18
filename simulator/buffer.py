"""
buffer.py - SQLite store-and-forward buffer.

Rule: every reading is written here BEFORE any publish attempt.
A row is marked synced only after the broker confirms it (PUBACK).
"""
import sqlite3
import time

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
CREATE TABLE IF NOT EXISTS telemetry_buffer (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms         INTEGER NOT NULL,   -- acquisition time, Unix epoch ms
    ph            REAL NOT NULL,
    turbidity_ntu REAL NOT NULL,
    tds_ppm       REAL NOT NULL,
    water_temp_c  REAL NOT NULL,
    flow_lpm      REAL NOT NULL,
    lat           REAL,
    lon           REAL,
    synced        INTEGER NOT NULL DEFAULT 0   -- 0 = waiting, 1 = confirmed
);
CREATE INDEX IF NOT EXISTS idx_unsynced ON telemetry_buffer(synced, ts_ms);
"""


class Buffer:
    def __init__(self, path="buffer.db", max_rows=12000):
        self.max_rows = max_rows
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript(SCHEMA)
        self.db.commit()

    def add(self, ts_ms, v):
        """Save one reading. v is the dict from the generator."""
        self.db.execute(
            "INSERT INTO telemetry_buffer "
            "(ts_ms, ph, turbidity_ntu, tds_ppm, water_temp_c, flow_lpm, lat, lon) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (ts_ms, v["ph"], v["turbidity_ntu"], v["tds_ppm"],
             v["water_temp_c"], v["flow_lpm"], v["latitude"], v["longitude"]))
        self.db.commit()
        return self._evict_if_full()

    def _evict_if_full(self):
        """Policy: when full, delete synced rows first, then the OLDEST unsynced."""
        total = self.total()
        if total <= self.max_rows:
            return 0
        excess = total - self.max_rows
        self.db.execute(
            "DELETE FROM telemetry_buffer WHERE id IN "
            "(SELECT id FROM telemetry_buffer WHERE synced=1 "
            " ORDER BY ts_ms LIMIT ?)", (excess,))
        left = self.total() - self.max_rows
        if left > 0:
            self.db.execute(
                "DELETE FROM telemetry_buffer WHERE id IN "
                "(SELECT id FROM telemetry_buffer WHERE synced=0 "
                " ORDER BY ts_ms LIMIT ?)", (left,))
        self.db.commit()
        return excess

    def next_batch(self, n=50):
        """Oldest unsynced rows first, as a list of dicts."""
        cur = self.db.execute(
            "SELECT id, ts_ms, ph, turbidity_ntu, tds_ppm, water_temp_c, "
            "flow_lpm, lat, lon "
            "FROM telemetry_buffer WHERE synced=0 ORDER BY ts_ms LIMIT ?", (n,))
        cols = ["id", "ts_ms", "ph", "turbidity_ntu", "tds_ppm",
                "water_temp_c", "flow_lpm", "lat", "lon"]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def mark_synced(self, ids):
        self.db.executemany(
            "UPDATE telemetry_buffer SET synced=1 WHERE id=?", [(i,) for i in ids])
        self.db.commit()

    def depth(self):
        """Rows still waiting to be sent (buffer depth)."""
        return self.db.execute(
            "SELECT COUNT(*) FROM telemetry_buffer WHERE synced=0").fetchone()[0]

    def total(self):
        return self.db.execute(
            "SELECT COUNT(*) FROM telemetry_buffer").fetchone()[0]


# ---------- self-test: python buffer.py ----------
if __name__ == "__main__":
    import os
    if os.path.exists("test.db"):
        os.remove("test.db")
    b = Buffer("test.db", max_rows=5)
    fake = {"ph": 7.1, "turbidity_ntu": 5.0, "tds_ppm": 200,
            "water_temp_c": 18.0, "flow_lpm": 12.0,
            "latitude": 27.9, "longitude": 85.1}
    now = int(time.time() * 1000)
    for k in range(8):                       # write 8 rows, cap is 5
        b.add(now + k * 1000, fake)
    print("total rows (cap 5):", b.total())
    batch = b.next_batch(3)
    print("oldest ts in batch:", batch[0]["ts_ms"] - now, "ms (should be 3000)")
    b.mark_synced([r["id"] for r in batch])
    print("depth after syncing 3:", b.depth())