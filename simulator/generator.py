"""
generator.py - virtual sensor model for one DHARA node.

One hidden variable, intrusion intensity I(t) in [0, 1], drives all coupled
parameters. Between events I = 0 and the node shows normal baseline values.
"""
import math
import random


class NodeGenerator:
    def __init__(self, name, lat, lon, seed=None):
        self.name = name
        self.lat = lat
        self.lon = lon
        self.rng = random.Random(seed)
        self.events = []          # list of intrusion events
        self.supply_cut = False   # True = flow forced to zero

        # Per-node baselines (inside the ranges given in the brief)
        self.base_ph = self.rng.uniform(7.0, 7.4)
        self.base_turb = self.rng.uniform(3.0, 6.0)
        self.base_tds = self.rng.uniform(150, 220)
        self.base_flow = self.rng.uniform(10, 16)

        # Model constants (justify these in the manuscript)
        self.K_TDS = 0.8        # ppm of TDS added per NTU of extra turbidity
        self.PH_DROP = 1.2      # max pH drop at full intensity
        self.PH_FLOOR = 6.2     # physical floor for pH
        self.FLOW_SURGE = 0.25  # +25% flow pressure transient at onset

    # ---------- scenario controls ----------
    def start_intrusion(self, t, peak_ntu=400, rise_s=180, tau_s=1800):
        """Start a silt/sewage event at time t (seconds)."""
        self.events.append({"t0": t, "peak": peak_ntu,
                            "rise": rise_s, "tau": tau_s})

    def set_supply_cut(self, on):
        self.supply_cut = on

    # ---------- the hidden variable ----------
    def intensity(self, t):
        """Return I(t) in [0,1] and the latest event's peak NTU."""
        best_i, best_peak = 0.0, 0.0
        for e in self.events:
            dt = t - e["t0"]
            if dt < 0:
                continue
            if dt <= e["rise"]:                      # fast rise (minutes)
                i = dt / e["rise"]
            else:                                    # slow exponential washout
                i = math.exp(-(dt - e["rise"]) / e["tau"])
            if i > best_i:
                best_i, best_peak = i, e["peak"]
        return best_i, best_peak

    # ---------- one reading ----------
    def read(self, t):
        """t = time in seconds (epoch). Returns a dict of rounded values."""
        i, peak = self.intensity(t)
        n = self.rng.gauss

        # Turbidity: baseline + event
        turb = self.base_turb + (peak - self.base_turb) * i + n(0, 0.3)
        turb = max(0.5, turb)

        # TDS: rises in proportion to the extra turbidity (coupling)
        tds = self.base_tds + self.K_TDS * (turb - self.base_turb) + n(0, 3)

        # soft floor: pH decays toward PH_FLOOR but never goes below it
        ph = self.PH_FLOOR + (self.base_ph - self.PH_FLOOR) * math.exp(-2.5 * i)
        ph = ph + n(0, 0.02)

        # Temperature: diurnal sine (coolest ~3am, warmest ~3pm) plus noise
        hour = (t / 3600.0) % 24
        temp = 20 + 4 * math.sin(2 * math.pi * (hour - 9) / 24) + n(0, 0.2)

        # Flow: normal variation, surge at event onset, or zero if cut
        flow = self.base_flow + n(0, 0.4)
        for e in self.events:
            dt = t - e["t0"]
            if dt >= 0:
                flow *= 1 + self.FLOW_SURGE * math.exp(-dt / 200.0)
        if self.supply_cut:
            flow = 0.0

        return {
            "ph": round(ph, 2),
            "turbidity_ntu": round(turb, 1),
            "tds_ppm": round(tds),
            "water_temp_c": round(temp, 1),
            "flow_lpm": round(max(0.0, flow), 1),
            "latitude": self.lat,
            "longitude": self.lon,
        }


# ---------- quick self-test: python generator.py ----------
if __name__ == "__main__":
    g = NodeGenerator("node1", 27.9124, 85.1582, seed=1)
    start = 1_794_614_400          # any starting time in seconds
    g.start_intrusion(start + 300, peak_ntu=400)
    print(" sec    ph    NTU   TDS  temp  flow")
    for s in range(0, 3600, 120):
        r = g.read(start + s)
        print(f"{s:4d} {r['ph']:5.2f} {r['turbidity_ntu']:6.1f} "
              f"{r['tds_ppm']:5d} {r['water_temp_c']:5.1f} {r['flow_lpm']:5.1f}")