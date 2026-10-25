"""Offline A/B readout: SRM gate, naive vs CUPED estimate, always-valid sequential p-values."""
import argparse
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from stats import cuped_adjust, diff_in_means, sequential_pvalues, srm_check  # noqa: E402


def load_users(path, max_users=None):
    users, seen = {}, set()
    with open(path) as f:
        for line in f:
            e = json.loads(line)
            if e["event_id"] in seen:                       # dedupe replays
                continue
            seen.add(e["event_id"])
            u = users.setdefault(e["user_id"], {"order": len(users)})
            if e["type"] == "assignment":
                u["variant"], u["pre"] = e["variant"], e["pre_metric"]
            else:
                u["y"] = e["value"]
    rows = sorted((u for u in users.values() if {"variant", "pre", "y"} <= u.keys()),
                  key=lambda r: r["order"])
    return rows[:max_users] if max_users else rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", default="data/events.jsonl")
    ap.add_argument("--max-users", type=int, default=None, help="analyze only the first N users")
    ap.add_argument("--tau-rel", type=float, default=0.2, help="mSPRT prior sd as a share of metric sd")
    args = ap.parse_args()

    rows = load_users(args.events, args.max_users)
    is_t = np.array([r["variant"] == "treatment" for r in rows])
    y = np.array([r["y"] for r in rows])
    x = np.array([r["pre"] for r in rows])

    srm = srm_check({"control": int((~is_t).sum()), "treatment": int(is_t.sum())},
                    {"control": 0.5, "treatment": 0.5})
    print(f"users={len(rows)}  control={(~is_t).sum()}  treatment={is_t.sum()}")
    verdict = "ALERT: do not trust effect estimates" if srm["alert"] else "ok"
    print(f"SRM chi2={srm['chi2']:.2f} p={srm['p_value']:.3g} -> {verdict}")

    naive = diff_in_means(y[is_t], y[~is_t])
    y_adj, theta = cuped_adjust(y, x)
    adj = diff_in_means(y_adj[is_t], y_adj[~is_t])
    vr = 1 - np.var(y_adj, ddof=1) / np.var(y, ddof=1)
    print(f"\nnaive : diff={naive['diff']:+.3f}  CI95=({naive['ci95'][0]:+.3f}, {naive['ci95'][1]:+.3f})  p={naive['p_value']:.3g}")
    print(f"CUPED : diff={adj['diff']:+.3f}  CI95=({adj['ci95'][0]:+.3f}, {adj['ci95'][1]:+.3f})  p={adj['p_value']:.3g}  (theta={theta:.2f}, variance reduction {vr:.0%})")

    checkpoints = list(range(500, len(rows) + 1, 500))
    print("\nalways-valid p-values (mSPRT, running min):")
    print(f"{'users':>7} {'naive':>10} {'CUPED':>10}")
    seq_n = dict(sequential_pvalues(y, is_t, (args.tau_rel * y.std()) ** 2, checkpoints))
    seq_a = dict(sequential_pvalues(y_adj, is_t, (args.tau_rel * y_adj.std()) ** 2, checkpoints))
    for k in checkpoints:
        if k in seq_n:
            print(f"{k:>7} {seq_n[k]:>10.4f} {seq_a[k]:>10.4f}")


if __name__ == "__main__":
    main()
