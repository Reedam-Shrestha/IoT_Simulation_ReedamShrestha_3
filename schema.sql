-- Edge store-and-forward buffer (SQLite, WAL mode)
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