"""Statistical building blocks: SRM check, CUPED, Welch diff, mSPRT sequential test."""
import numpy as np
from scipy import stats


def srm_check(observed, expected_ratios, alpha=0.001, min_n=500):
    """Chi-square sample ratio mismatch test.

    observed: {variant: count}; expected_ratios: {variant: share}.
    Uses a strict alpha (0.001) because SRM is checked repeatedly on a live stream.
    """
    variants = sorted(observed)
    obs = np.array([observed[v] for v in variants], dtype=float)
    n = obs.sum()
    if n < min_n:
        return {"n": int(n), "chi2": None, "p_value": None, "alert": False}
    ratios = np.array([expected_ratios[v] for v in variants], dtype=float)
    ratios = ratios / ratios.sum()
    chi2, p = stats.chisquare(obs, n * ratios)
    return {"n": int(n), "chi2": float(chi2), "p_value": float(p), "alert": bool(p < alpha)}


def cuped_adjust(y, x):
    """CUPED: y_adj = y - theta * (x - mean(x)), theta = cov(x, y) / var(x), pooled over both arms.

    x must be a pre-experiment covariate, so it cannot be affected by treatment.
    """
    y = np.asarray(y, dtype=float)
    x = np.asarray(x, dtype=float)
    theta = np.cov(x, y, ddof=1)[0, 1] / np.var(x, ddof=1)
    return y - theta * (x - x.mean()), float(theta)


def diff_in_means(y_t, y_c):
    """Welch difference in means with a 95% CI."""
    y_t, y_c = np.asarray(y_t, float), np.asarray(y_c, float)
    diff = y_t.mean() - y_c.mean()
    se = np.sqrt(y_t.var(ddof=1) / len(y_t) + y_c.var(ddof=1) / len(y_c))
    _, p = stats.ttest_ind(y_t, y_c, equal_var=False)
    return {"diff": float(diff), "se": float(se), "ci95": (float(diff - 1.96 * se), float(diff + 1.96 * se)),
            "p_value": float(p)}


def msprt_p(y_t, y_c, tau2):
    """Mixture SPRT (normal mixing prior N(0, tau2) on the true difference).

    Likelihood ratio: sqrt(V / (V + tau2)) * exp(tau2 * d^2 / (2 V (V + tau2))),
    where d is the observed difference and V its estimated variance.
    Returns min(1, 1 / LR). Take a running minimum over time for an always-valid p-value.
    """
    y_t, y_c = np.asarray(y_t, float), np.asarray(y_c, float)
    d = y_t.mean() - y_c.mean()
    v = y_t.var(ddof=1) / len(y_t) + y_c.var(ddof=1) / len(y_c)
    log_lr = 0.5 * np.log(v / (v + tau2)) + tau2 * d**2 / (2 * v * (v + tau2))
    return float(min(1.0, np.exp(-log_lr)))


def sequential_pvalues(y, is_treat, tau2, checkpoints, min_per_arm=30):
    """Always-valid p-value at each checkpoint, using users in arrival order."""
    y = np.asarray(y, float)
    t = np.asarray(is_treat, bool)
    best, out = 1.0, []
    for k in checkpoints:
        yt, yc = y[:k][t[:k]], y[:k][~t[:k]]
        if len(yt) < min_per_arm or len(yc) < min_per_arm:
            continue
        best = min(best, msprt_p(yt, yc, tau2))
        out.append((int(k), best))
    return out
