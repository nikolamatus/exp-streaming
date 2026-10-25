import os
import sys
from datetime import datetime, timezone

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from generator import make_user_events  # noqa: E402
from stats import cuped_adjust, msprt_p, sequential_pvalues, srm_check  # noqa: E402
from windows import WindowState  # noqa: E402


def test_srm_quiet_when_balanced():
    assert not srm_check({"a": 2510, "b": 2490}, {"a": 1, "b": 1})["alert"]


def test_srm_fires_when_skewed():
    assert srm_check({"a": 2000, "b": 3000}, {"a": 1, "b": 1})["alert"]


def test_srm_waits_for_min_n():
    assert not srm_check({"a": 10, "b": 90}, {"a": 1, "b": 1})["alert"]


def test_cuped_reduces_variance_and_keeps_mean():
    rng = np.random.default_rng(1)
    x = rng.normal(10, 3, 20000)
    y = 0.7 * x + rng.normal(0, 2.1, 20000) + 5        # corr(x, y) about 0.7
    y_adj, _ = cuped_adjust(y, x)
    reduction = 1 - y_adj.var() / y.var()
    assert 0.35 < reduction < 0.65
    assert abs(y_adj.mean() - y.mean()) < 1e-9


def test_msprt_false_positive_rate_under_null_with_continuous_peeking():
    rng = np.random.default_rng(3)
    hits = 0
    for _ in range(300):
        y = rng.normal(0, 1, 2000)
        t = rng.random(2000) < 0.5
        seq = sequential_pvalues(y, t, tau2=0.04, checkpoints=range(100, 2001, 100))
        hits += seq[-1][1] < 0.05
    assert hits / 300 < 0.08


def test_msprt_detects_real_effect():
    rng = np.random.default_rng(4)
    y_t, y_c = rng.normal(0.3, 1, 3000), rng.normal(0, 1, 3000)
    assert msprt_p(y_t, y_c, tau2=0.04) < 0.01


def test_window_state_replay_is_idempotent():
    s = WindowState()
    batch = [("w1", "a", 10, 50.0), ("w1", "b", 12, 60.0)]
    s.apply(batch)
    s.apply(batch)
    s.apply([("w1", "a", 15, 80.0)])                      # update mode re-emits the full window value
    assert s.totals()["a"]["users"] == 15
    assert s.totals()["b"]["users"] == 12


def test_generator_bug_skews_assignment():
    rng = np.random.default_rng(5)
    now = datetime.now(timezone.utc)
    ok = [make_user_events(i, rng, now, bug_active=False)[0]["variant"] for i in range(4000)]
    bad = [make_user_events(i, rng, now, bug_active=True)[0]["variant"] for i in range(4000)]
    assert abs(ok.count("treatment") / 4000 - 0.5) < 0.03
    assert abs(bad.count("treatment") / 4000 - 0.62) < 0.03
