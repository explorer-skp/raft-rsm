#!/usr/bin/env python3
"""Regenerates the Phase 8 plots and summary tables from saved raw data.

Usage: plot_results.py RESULTS_DIR

Reads the JSON files written by rsm_bench (via run_benchmarks.sh), writes
PNGs into RESULTS_DIR/plots/ and a text summary into RESULTS_DIR/summary.txt.
Never re-runs anything: the saved files are the source of truth.
"""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

US = 1e3  # ns -> us divisor


def load(outdir: Path):
    runs = defaultdict(list)  # label -> [doc...]
    for f in sorted(outdir.glob("*.json")):
        d = json.loads(f.read_text())
        runs[d["label"]].append(d)
    return runs


def med(vals):
    return statistics.median(vals) if vals else float("nan")


def medres(docs, key):
    return med([d["results"][key] for d in docs])


def medhist(docs, hist, key):
    vals = [d["results"][hist][key] for d in docs if d["results"][hist]["count"]]
    return med(vals) if vals else float("nan")


def quantile_curve(doc, hist):
    qs = doc["results"][hist]["quantiles"]
    return [q for q, _ in qs], [v / US for _, v in qs]


def nines_axis(ax):
    ticks = [0.5, 0.9, 0.99, 0.999, 0.9999, 0.99999]
    ax.set_xscale("log")
    ax.set_xticks([1 / (1 - q) for q in ticks])
    ax.set_xticklabels(["p50", "p90", "p99", "p99.9", "p99.99", "p99.999"])
    ax.minorticks_off()


def plot_latency_vs_throughput(runs, plots: Path):
    fig, ax = plt.subplots(figsize=(7, 5))
    any_data = False
    for cfg, style in (("open.base", "o-"), ("open.perf", "s--"),
                       ("open.best", "^:")):
        groups = {
            lbl: docs for lbl, docs in runs.items()
            if lbl.startswith(cfg + ".rate")
        }
        if not groups:
            continue
        pts = []
        for lbl, docs in groups.items():
            tput = medres(docs, "throughput_cps")
            pts.append((
                tput,
                medhist(docs, "e2e_intended", "p50_ns") / US,
                medhist(docs, "e2e_intended", "p99_ns") / US,
                medhist(docs, "e2e_intended", "p999_ns") / US,
            ))
        pts.sort()
        any_data = True
        xs = [p[0] for p in pts]
        for idx, name in ((1, "p50"), (2, "p99"), (3, "p99.9")):
            ax.plot(xs, [p[idx] for p in pts], style,
                    label=f"{cfg.split('.')[1]} {name}")
    if not any_data:
        plt.close(fig)
        return
    ax.set_yscale("log")
    ax.set_xlabel("achieved committed ops/s")
    ax.set_ylabel("end-to-end latency from intended send (us)")
    ax.set_title("Latency vs throughput (open-loop, CO-corrected)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(plots / "latency_vs_throughput.png", dpi=130)
    plt.close(fig)


def plot_cdf(runs, plots: Path):
    fig, ax = plt.subplots(figsize=(7, 5))
    any_data = False
    for cfg in ("headline.base", "headline.perf", "headline.best"):
        docs = runs.get(cfg)
        if not docs:
            continue
        doc = docs[0]
        for hist, ls in (("e2e_intended", "-"), ("commit", "--")):
            if not doc["results"][hist]["count"]:
                continue
            qs, vals = quantile_curve(doc, hist)
            xs = [1 / (1 - q) if q < 1 else 1e6 for q in qs]
            ax.plot(xs, vals, ls,
                    label=f"{cfg.split('.')[1]} {hist} "
                          f"@{doc['config']['rate']:.0f}/s")
            any_data = True
    if not any_data:
        plt.close(fig)
        return
    nines_axis(ax)
    ax.set_yscale("log")
    ax.set_ylabel("latency (us)")
    ax.set_title("Latency percentiles at the stated offered load")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(plots / "latency_percentiles.png", dpi=130)
    plt.close(fig)


def plot_batch(runs, plots: Path):
    fig, ax = plt.subplots(figsize=(7, 5))
    any_data = False
    series = {
        "tmpfs block": ("closed.c16.block.b", "o-"),
        "tmpfs spin": ("closed.c16.spin.b", "s-"),
        "disk block": ("disk.c16.block.b", "^--"),
    }
    for name, (prefix, style) in series.items():
        pts = []
        for lbl, docs in runs.items():
            if not lbl.startswith(prefix):
                continue
            rest = lbl[len(prefix):]
            if not rest.isdigit():  # skips the fsgroup equivalence cell
                continue
            pts.append((int(rest), medres(docs, "throughput_cps")))
        if not pts:
            continue
        pts.sort()
        ax.plot([p[0] for p in pts], [p[1] for p in pts], style, label=name)
        any_data = True
    if not any_data:
        plt.close(fig)
        return
    ax.set_xscale("log", base=2)
    ax.set_xlabel("group-commit batch size")
    ax.set_ylabel("committed ops/s (16 closed-loop clients)")
    ax.set_title("Throughput vs batch size")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(plots / "throughput_vs_batch.png", dpi=130)
    plt.close(fig)


def plot_clients(runs, plots: Path):
    fig, ax = plt.subplots(figsize=(7, 5))
    any_data = False
    for wait, style in (("block", "o-"), ("spin", "s-")):
        pts = []
        for lbl, docs in runs.items():
            if not (lbl.startswith("closed.c") and lbl.endswith(f".{wait}.b1")):
                continue
            c = int(lbl.split(".")[1][1:])
            pts.append((c, medres(docs, "throughput_cps")))
        if not pts:
            continue
        pts.sort()
        ax.plot([p[0] for p in pts], [p[1] for p in pts], style, label=wait)
        any_data = True
    if not any_data:
        plt.close(fig)
        return
    ax.set_xscale("log", base=2)
    ax.set_xlabel("closed-loop clients")
    ax.set_ylabel("committed ops/s")
    ax.set_title("Throughput vs concurrency (batch off)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(plots / "throughput_vs_clients.png", dpi=130)
    plt.close(fig)


def plot_failover(runs, plots: Path):
    docs = runs.get("failover")
    if not docs:
        return
    ms = docs[0]["results"]["failover_ms"]
    if not ms:
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.hist(ms, bins=24, edgecolor="black", alpha=0.8)
    s = sorted(ms)
    p50 = s[len(s) // 2]
    p99 = s[min(len(s) - 1, int(len(s) * 0.99))]
    ax.axvline(p50, color="green", ls="--", label=f"p50 = {p50:.0f} ms")
    ax.axvline(p99, color="orange", ls="--", label=f"p99 = {p99:.0f} ms")
    ax.axvline(max(s), color="red", ls=":", label=f"max = {max(s):.0f} ms")
    ax.set_xlabel("failover time (ms): leader killed -> next committed write")
    ax.set_ylabel(f"trials (n={len(ms)})")
    ax.set_title("Failover-time distribution under load")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(plots / "failover_distribution.png", dpi=130)
    plt.close(fig)


def plot_faults(runs, plots: Path):
    labels, tput, p99 = [], [], []
    for name, lbl in (
        ("clean", "fault.open.clean"),
        ("1% loss", "fault.open.loss1"),
        ("5% loss", "fault.open.loss5"),
        ("10% loss", "fault.open.loss10"),
        ("partition\n1s/5s", "fault.open.partition"),
    ):
        docs = runs.get(lbl)
        if not docs:
            continue
        labels.append(name)
        tput.append(medres(docs, "throughput_cps"))
        p99.append(medhist(docs, "e2e_intended", "p99_ns") / US)
    if not labels:
        return
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))
    ax1.bar(labels, tput, color="steelblue", edgecolor="black")
    ax1.set_ylabel("achieved committed ops/s")
    ax1.set_title("Throughput under sustained faults\n(fixed offered load)")
    ax1.grid(True, axis="y", alpha=0.3)
    ax2.bar(labels, p99, color="indianred", edgecolor="black")
    ax2.set_yscale("log")
    ax2.set_ylabel("e2e p99 from intended send (us)")
    ax2.set_title("p99 latency under sustained faults")
    ax2.grid(True, axis="y", which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(plots / "faults.png", dpi=130)
    plt.close(fig)


def hist_row(docs, hist):
    def v(key):
        x = medhist(docs, hist, key)
        return f"{x / US:10.0f}" if x == x else "       n/a"

    return (f"{v('p50_ns')} {v('p99_ns')} {v('p999_ns')} {v('p9999_ns')} "
            f"{v('max_ns')}")


def write_summary(runs, outdir: Path):
    lines = []
    a = lines.append
    machine = next(iter(runs.values()))[0]["machine_start"]
    # Shared by the Phase 8 KV suite and the Phase 9 order-book suite; the
    # state machine is recorded per run, so name it in the title.
    sms = {d["config"].get("sm", "kv") for docs in runs.values() for d in docs}
    workload = "/".join(sorted(sms)).upper()
    a(f"BENCHMARK SUMMARY — {workload} workload "
      "(medians of repeats; latencies in us)")
    a(f"host: {machine['cpu_model']} | {machine['cpus']} cpus | "
      f"governor={machine['governors']} no_turbo={machine['intel_pstate_no_turbo']} | "
      f"{machine['kernel']}")
    a("")

    def section(title):
        a(title)
        a("-" * len(title))

    section("Throughput sweeps (committed ops/s)")
    a(f"{'config':40s} {'ops/s':>10s} {'e2e p50':>9s} {'e2e p99':>9s}")
    for lbl in sorted(runs):
        if not (lbl.startswith("closed.") or lbl.startswith("disk.")
                or lbl.startswith("pin_ab")):
            continue
        docs = runs[lbl]
        a(f"{lbl:40s} {medres(docs, 'throughput_cps'):10.0f} "
          f"{medhist(docs, 'e2e_actual', 'p50_ns') / US:9.0f} "
          f"{medhist(docs, 'e2e_actual', 'p99_ns') / US:9.0f}")
    a("")

    section("Open-loop rate sweep (CO-corrected e2e latency)")
    a(f"{'config':28s} {'offered':>8s} {'achieved':>9s} "
      f"{'p50':>8s} {'p99':>8s} {'p99.9':>8s} {'abandoned':>9s}")
    for lbl in sorted(runs, key=lambda s: (s.rsplit("rate", 1)[0],
                                           int(s.rsplit("rate", 1)[1])
                                           if "rate" in s and
                                           s.rsplit("rate", 1)[1].isdigit()
                                           else 0)):
        if ".rate" not in lbl or not lbl.startswith("open."):
            continue
        docs = runs[lbl]
        a(f"{lbl:28s} {docs[0]['config']['rate']:8.0f} "
          f"{medres(docs, 'throughput_cps'):9.0f} "
          f"{medhist(docs, 'e2e_intended', 'p50_ns') / US:8.0f} "
          f"{medhist(docs, 'e2e_intended', 'p99_ns') / US:8.0f} "
          f"{medhist(docs, 'e2e_intended', 'p999_ns') / US:8.0f} "
          f"{medres(docs, 'abandoned'):9.0f}")
    a("")

    section("Headline latency (stated offered load; us)")
    a(f"{'series':34s} {'p50':>10s} {'p99':>10s} {'p99.9':>10s} "
      f"{'p99.99':>10s} {'max':>10s}")
    for cfg in ("headline.base", "headline.perf", "headline.best"):
        docs = runs.get(cfg)
        if not docs:
            continue
        rate = docs[0]["config"]["rate"]
        n = medres(docs, "oks")
        a(f"{cfg} @{rate:.0f}/s ({n:.0f} samples/run)")
        a(f"{'  e2e (intended send)':34s} {hist_row(docs, 'e2e_intended')}")
        a(f"{'  e2e (actual send)':34s} {hist_row(docs, 'e2e_actual')}")
        a(f"{'  commit (leader internal)':34s} {hist_row(docs, 'commit')}")
    a("")

    for name in ("stress", "failover"):
        docs = runs.get(name)
        if not docs:
            continue
        r = docs[0]["results"]
        if name == "stress":
            section("Stress regime (32 clients, batch 8, spin, sustained)")
            a(f"throughput={r['throughput_cps']:.0f} ops/s  "
              f"elections_in_window={r['elections_in_window']}  "
              f"max_term={r['max_term']}  valid={r['valid']}")
        else:
            section("Failover (leader kill under load)")
            ms = sorted(r["failover_ms"])
            if ms:
                a(f"n={len(ms)}  p50={ms[len(ms)//2]:.0f}ms  "
                  f"p99={ms[min(len(ms)-1, int(len(ms)*0.99))]:.0f}ms  "
                  f"max={ms[-1]:.0f}ms")
        a("")

    # Only emitted when fault cells exist in this result set (the Phase 9
    # order-book suite has none: fault behavior is SM-independent and is
    # measured once, in the Phase 8 set).
    if any(lbl.startswith("fault.") for lbl in runs):
        section("Sustained faults (open-loop at fixed offered load)")
        a(f"{'config':26s} {'achieved/s':>10s} {'p50':>8s} {'p99':>8s} "
          f"{'elections':>9s}")
        for lbl in sorted(runs):
            if not lbl.startswith("fault."):
                continue
            docs = runs[lbl]
            hist = "e2e_intended" if ".open." in lbl else "e2e_actual"
            a(f"{lbl:26s} {medres(docs, 'throughput_cps'):10.0f} "
              f"{medhist(docs, hist, 'p50_ns') / US:8.0f} "
              f"{medhist(docs, hist, 'p99_ns') / US:8.0f} "
              f"{medres(docs, 'elections_in_window'):9.0f}")
        a("")

    invalid = [lbl for lbl, docs in runs.items()
               if any(not d["results"]["valid"] for d in docs)]
    a(f"invalid runs: {invalid if invalid else 'none'}")
    (outdir / "summary.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    outdir = Path(sys.argv[1])
    plots = outdir / "plots"
    plots.mkdir(exist_ok=True)
    runs = load(outdir)
    if not runs:
        print(f"no JSON results in {outdir}", file=sys.stderr)
        sys.exit(1)
    plot_latency_vs_throughput(runs, plots)
    plot_cdf(runs, plots)
    plot_batch(runs, plots)
    plot_clients(runs, plots)
    plot_failover(runs, plots)
    plot_faults(runs, plots)
    write_summary(runs, outdir)
    print(f"plots in {plots}")


if __name__ == "__main__":
    main()
