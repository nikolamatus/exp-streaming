import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from kafka_stream import LiveAggregator, format_report  # noqa: E402


def feed(agg, n_control, n_treatment, value=1.0):
    reports = []
    for i in range(n_control + n_treatment):
        variant = "control" if i < n_control else "treatment"
        for kind in ("assignment", "engagement"):
            r = agg.update({"variant": variant, "type": kind, "value": value if kind == "engagement" else 0.0})
            if r:
                reports.append(r)
    return reports


def interleaved(agg, pattern, n):
    out = []
    for i in range(n):
        variant = pattern(i)
        for kind in ("assignment", "engagement"):
            r = agg.update({"variant": variant, "type": kind, "value": 2.0})
            if r:
                out.append(r)
    return out


def test_balanced_stream_never_alerts():
    agg = LiveAggregator(report_every=500)
    reports = interleaved(agg, lambda i: "control" if i % 2 == 0 else "treatment", 4000)
    assert reports and not any(r["srm"]["alert"] for r in reports)


def test_skewed_stream_alerts():
    agg = LiveAggregator(report_every=500)
    reports = interleaved(agg, lambda i: "treatment" if i % 10 < 7 else "control", 4000)   # 70/30
    assert reports[-1]["srm"]["alert"]


def test_final_report_includes_every_event():
    agg = LiveAggregator(report_every=500)
    reports = interleaved(agg, lambda i: "control" if i % 2 == 0 else "treatment", 2000)
    assert reports[-1]["events"] == 4000 and reports[-1]["users"] == 2000


def test_means_and_ignored_events():
    agg = LiveAggregator(report_every=2)
    assert agg.update({"variant": "other", "type": "assignment"}) is None
    assert agg.update({"variant": "control", "type": "unknown"}) is None
    assert agg.processed == 0
    reports = feed(agg, 1, 1, value=3.0)
    assert reports and reports[-1]["means"] == {"control": 3.0, "treatment": 3.0}


def test_small_report_interval_does_not_crash_formatting():
    agg = LiveAggregator(report_every=2)           # n below the SRM minimum, so p_value is None
    reports = feed(agg, 2, 2)
    assert "n/a" in format_report(reports[0])
