"""Driver-side state for per-window, per-variant aggregates emitted by Spark in update mode.

Update mode re-emits the full current value of every window touched in a micro-batch, so
applying rows is an idempotent overwrite keyed by (window_start, variant). Replaying a batch
does not double count.
"""


class WindowState:
    def __init__(self):
        self._w = {}

    def apply(self, rows):
        """rows: iterable of (window_start, variant, users, engagement_sum)."""
        for start, variant, users, total in rows:
            self._w[(start, variant)] = (int(users), float(total))

    def totals(self):
        acc = {}
        for (_, variant), (users, total) in self._w.items():
            a = acc.setdefault(variant, [0, 0.0])
            a[0] += users
            a[1] += total
        return {v: {"users": u, "engagement_sum": s, "mean_engagement": (s / u if u else 0.0)}
                for v, (u, s) in acc.items()}
