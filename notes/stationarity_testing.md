# Stationarity Testing in Financial Time Series

## Core distinction: serial correlation ≠ non-stationarity

A series can be **stationary and highly autocorrelated at the same time**.

The canonical example is a stationary AR(1):

```
xₜ = ρ xₜ₋₁ + εₜ,   |ρ| < 1
```

With ρ = 0.97 this process has a finite, constant mean and variance — it is
technically stationary — yet its ACF decays very slowly and looks almost
indistinguishable from a unit root visually. This is persistent but stationary.

---

## ADF (Augmented Dickey-Fuller)

**H₀**: the series has a unit root (ρ = 1, i.e. non-stationary).  
**Reject H₀** (p < 0.05) → evidence of stationarity.

### What it actually tests
Only one specific type of non-stationarity: a stochastic trend (random walk).
It does **not** test for:
- Non-constant variance (heteroscedasticity / volatility clustering)
- Structural level shifts or regime changes
- Long memory (Hurst H > 0.5)

### Key limitation: low power near ρ = 1
When the true ρ is very close to 1 (e.g. 0.999 for a price series), ADF
frequently fails to reject the unit root null — it "passes" the series as
stationary even when it clearly isn't. This is the near-unit-root problem.

The "augmented" lags absorb serial correlation in the test regression but do
not fully resolve the size distortion at near-unit-root persistence levels.

---

## KPSS (Kwiatkowski-Phillips-Schmidt-Shin)

**H₀**: the series is stationary (constant mean, constant variance).  
**Reject H₀** (p < 0.05) → evidence of non-stationarity.

### What it actually tests
Covariance (weak) stationarity. It is sensitive to **any** departure from a
constant mean and constant variance, including:
- Unit roots (same as ADF)
- Structural level shifts
- Non-constant variance (GARCH / volatility clustering)
- Long memory (Hurst H > 0.5)

### Why KPSS catches more in financial data
Financial microstructure features violate stationarity through heteroscedasticity,
regime shifts, and long memory far more often than through a clean unit root.
KPSS is sensitive to all of these; ADF is not.

| Feature type | Why KPSS rejects |
|---|---|
| `kyle_lambda`, `realized_vol`, `ewm_vol` | Volatility clustering — variance is not constant |
| `vpin_50`, `vpin_10` | Slowly-varying rolling mean — behaves like a level shift |
| `close` | Near-unit-root trend that ADF lacks power to detect |
| `duration_s` | Regime-dependent duration across high/low volatility periods |

---

## Interpreting both tests together

When ADF and KPSS disagree, the safest interpretation in finance is to treat
the series as non-stationary and apply a transformation.

| ADF | KPSS | Interpretation | Action |
|---|---|---|---|
| ✓ stationary | ✓ stationary | Both agree — likely safe | Use as-is |
| ✓ stationary | ✗ non-stationary | Persistent or heteroscedastic | Transform or use with care |
| ✗ non-stationary | ✗ non-stationary | Unit root — both agree | Difference or fracdiff |
| ✗ non-stationary | ✓ stationary | Unusual; small-sample noise | Investigate further |

The middle row (ADF passes, KPSS rejects) is the most common case in
microstructure data. KPSS is doing the real diagnostic work there.

---

## What the ACF tells you (beyond the tests)

A slowly decaying ACF signals **persistence / long memory**, which is distinct
from non-stationarity but equally important for modeling:

| ACF pattern | Implication |
|---|---|
| Decays to 0 in a few lags | Weakly dependent — safe to use as-is |
| Decays slowly (exponential) | High persistence — AR lags needed, or fractional differencing |
| Decays linearly / never reaches 0 | True unit root — needs differencing |

The Hurst exponent (R/S analysis) quantifies this:
- **H < 0.45** — mean-reverting
- **H ≈ 0.5** — random walk / no memory
- **H > 0.55** — trending / long memory

For ML features, H > 0.55 is a practical warning even when both ADF and KPSS
pass: the feature carries persistent structure that can dominate a model or
cause data leakage through look-ahead in rolling statistics.

---

## Practical transformation guide

| Condition | Transformation |
|---|---|
| Unit root (price-like) | Log-differencing: `log(pₜ / pₜ₋₁)` |
| Near-unit-root, high Hurst | Fractional differencing (preserves memory while removing trend) |
| Non-constant variance only | Standardise with rolling z-score, or model variance explicitly (GARCH) |
| Bounded series, high persistence | Usually safe — cannot have a unit root; consider inclusion as-is or lagged |
