#!/usr/bin/env python3
"""Picks the headline offered load from a saved open-loop rate sweep.

Usage: pick_rate.py RESULTS_DIR PREFIX  (e.g. open.base)

Sustainable = the highest swept rate where every repeat achieved >= 99% of
the offered send rate, abandoned nothing, stayed valid, AND kept the
CO-corrected e2e p99 under 50 ms — the last clause matters: a bounded
in-flight generator can keep pace with the offered rate while per-request
queueing grows toward seconds, so rate fidelity alone does not mean "below
the knee". The headline load is 70% of that (a stated,
comfortably-inside-the-knee operating point). Falls back conservatively if
nothing qualifies. Prints one integer.
"""

import json
import sys
from pathlib import Path


def main() -> None:
    out, prefix = Path(sys.argv[1]), sys.argv[2]
    by_rate: dict[float, list[dict]] = {}
    for f in out.glob(f"{prefix}.rate*.r*.json"):
        d = json.loads(f.read_text())
        by_rate.setdefault(d["config"]["rate"], []).append(d["results"])

    sustainable = 0.0
    for rate in sorted(by_rate):
        runs = by_rate[rate]
        ok = all(
            r["valid"]
            and r["abandoned"] == 0
            and r["achieved_send_rate"] >= 0.99 * rate
            and r["throughput_cps"] >= 0.99 * rate
            and r["e2e_intended"]["p99_ns"] <= 50e6
            and r["e2e_intended"]["p999_ns"] <= 20e6
            for r in runs
        )
        if ok:
            sustainable = rate
    if sustainable <= 0:
        sustainable = min(by_rate) if by_rate else 2000.0
    print(int(sustainable * 0.7))


if __name__ == "__main__":
    main()
