from __future__ import annotations

import calendar
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd


METHOD_VERSION = "wapor-npp-literature-v1"
PHENOLOGY_VERSION = "optical-rvi-relative-threshold-v1"
NPP_TO_TBP = 0.022222


@dataclass(frozen=True)
class CropParameters:
    crop: str
    fc: tuple[float, float, float]
    aot: tuple[float, float, float]
    hi: tuple[float, float, float]
    moisture: tuple[float, float, float]
    output_basis: str
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


CROP_PARAMETERS = {
    "maize": CropParameters(
        "maize", (1.8, 1.8, 1.8), (0.65, 0.65, 0.65),
        (0.48, 0.50, 0.52), (0.10, 0.125, 0.15),
        "commercial grain fresh-weight", "starting",
    ),
    "beans": CropParameters(
        "beans", (1.0, 1.0, 1.0), (0.65, 0.65, 0.65),
        (0.38, 0.50, 0.85), (0.12, 0.14, 0.15),
        "dry bean grain at stated moisture", "starting-wide-range",
    ),
    "potatoes": CropParameters(
        "potatoes", (1.0, 1.0, 1.0), (1.0, 1.0, 1.0),
        (0.70, 0.79, 0.85), (0.75, 0.79, 0.80),
        "fresh tuber weight", "starting",
    ),
}


def season_bounds(season_code: str) -> tuple[date, date]:
    try:
        year_text, kind = season_code.lower().split("_", 1)
        year = int(year_text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid season code: {season_code!r}") from exc
    if kind == "lr":
        return date(year, 3, 1), date(year, 8, 31)
    if kind == "sr":
        return date(year, 10, 1), date(year + 1, 2, calendar.monthrange(year + 1, 2)[1])
    raise ValueError(f"Unknown season kind in {season_code!r}")


def dekads_between(start: date, end: date) -> list[dict[str, Any]]:
    if end < start:
        raise ValueError("end must be on or after start")
    result: list[dict[str, Any]] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        last = calendar.monthrange(cursor.year, cursor.month)[1]
        for number, (day_start, day_end) in enumerate(((1, 10), (11, 20), (21, last)), 1):
            period_start = date(cursor.year, cursor.month, day_start)
            period_end = date(cursor.year, cursor.month, day_end)
            if period_end < start or period_start > end:
                continue
            code = f"WAPOR-3.L2-NPP-D.{cursor.year:04d}-{cursor.month:02d}-D{number}"
            result.append(
                {
                    "code": code,
                    "period_start": period_start,
                    "period_end": period_end,
                    "days_in_period": (period_end - period_start).days + 1,
                    "dekad": number,
                }
            )
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return result


def overlap_days(period_start: date, period_end: date, sos: date, eos: date) -> int:
    left = max(period_start, sos)
    right = min(period_end, eos)
    return max(0, (right - left).days + 1)


def calculate_yield(tbp_tdm_ha: float, crop: str) -> dict[str, float]:
    params = CROP_PARAMETERS[crop.lower()]
    names = ("low", "central", "high")
    values: dict[str, float] = {}
    for i, name in enumerate(names):
        values[name] = (
            tbp_tdm_ha
            * params.fc[i]
            * params.aot[i]
            * params.hi[i]
            / (1.0 - params.moisture[i])
        )
    return values


def _robust_normalize(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    valid = numeric.dropna()
    if len(valid) < 3:
        return pd.Series(np.nan, index=values.index, dtype=float)
    low, high = valid.quantile([0.1, 0.9])
    if not np.isfinite(low) or not np.isfinite(high) or high - low <= 1e-8:
        return pd.Series(np.nan, index=values.index, dtype=float)
    return ((numeric - low) / (high - low)).clip(0, 1)


def derive_phenology(
    weekly: pd.DataFrame,
    season_start: date,
    season_end: date,
    *,
    threshold_fraction: float = 0.20,
    minimum_optical_weeks: int = 6,
) -> dict[str, Any]:
    """Derive an auditable SOS/EOS estimate from existing weekly S1/S2 features.

    This is an intentionally conservative pilot heuristic. It uses a relative
    threshold around the seasonal peak and records a fallback instead of
    inventing dates when the time series is insufficient.
    """
    frame = weekly.copy()
    if frame.empty:
        return {
            "sos": season_start, "peak_date": None, "eos": season_end,
            "method": "full-season-fallback", "confidence": 0.0,
            "qa_status": "no_weekly_signal", "optical_valid_weeks": 0,
            "sar_supported_weeks": 0,
        }
    frame["week_start"] = pd.to_datetime(frame["week_start"])
    frame = frame.sort_values("week_start").drop_duplicates("week_start", keep="last")
    frame = frame[
        (frame["week_start"].dt.date >= season_start)
        & (frame["week_start"].dt.date <= season_end)
    ].copy()
    optical_cols = [c for c in ("ndvi_median", "evi_median", "lai_median") if c in frame]
    normalized = [_robust_normalize(frame[c]) for c in optical_cols]
    if normalized:
        optical = pd.concat(normalized, axis=1).median(axis=1, skipna=True)
    else:
        optical = pd.Series(np.nan, index=frame.index, dtype=float)
    optical_valid = int(optical.notna().sum())
    sar_supported = 0
    signal = optical.copy()
    if "rvi_median" in frame:
        rvi = _robust_normalize(frame["rvi_median"])
        sar_supported = int((signal.isna() & rvi.notna()).sum())
        signal = signal.fillna(rvi)
    signal = signal.interpolate(limit=2, limit_direction="both")
    signal = signal.rolling(3, center=True, min_periods=1).median()
    valid = signal.dropna()
    if optical_valid < minimum_optical_weeks or len(valid) < minimum_optical_weeks:
        return {
            "sos": season_start, "peak_date": None, "eos": season_end,
            "method": "full-season-fallback", "confidence": 0.15 if len(valid) else 0.0,
            "qa_status": "insufficient_optical_weeks", "optical_valid_weeks": optical_valid,
            "sar_supported_weeks": sar_supported,
        }
    peak_idx = valid.idxmax()
    peak_value = float(valid.loc[peak_idx])
    edge = pd.concat([valid.head(3), valid.tail(3)])
    baseline = float(edge.median())
    amplitude = peak_value - baseline
    if not np.isfinite(amplitude) or amplitude < 0.20:
        return {
            "sos": season_start, "peak_date": frame.loc[peak_idx, "week_start"].date(),
            "eos": season_end, "method": "full-season-fallback", "confidence": 0.2,
            "qa_status": "weak_seasonal_amplitude", "optical_valid_weeks": optical_valid,
            "sar_supported_weeks": sar_supported,
        }
    threshold = baseline + threshold_fraction * amplitude
    before = frame.loc[valid.index[valid.index <= peak_idx]].copy()
    before["above"] = signal.loc[before.index] >= threshold
    after = frame.loc[valid.index[valid.index >= peak_idx]].copy()
    after["below"] = signal.loc[after.index] <= threshold

    sos_idx = None
    for i in range(max(0, len(before) - 1)):
        if bool(before.iloc[i]["above"]) and bool(before.iloc[i + 1]["above"]):
            sos_idx = before.index[i]
            break
    eos_idx = None
    for i in range(max(0, len(after) - 1)):
        if bool(after.iloc[i]["below"]) and bool(after.iloc[i + 1]["below"]):
            eos_idx = after.index[i]
            break

    sos = frame.loc[sos_idx, "week_start"].date() if sos_idx is not None else season_start
    eos = frame.loc[eos_idx, "week_start"].date() if eos_idx is not None else season_end
    peak_date = frame.loc[peak_idx, "week_start"].date()
    if sos > peak_date or eos < peak_date or sos >= eos:
        sos, eos, sos_idx, eos_idx = season_start, season_end, None, None
    both = sos_idx is not None and eos_idx is not None
    coverage_score = min(1.0, optical_valid / max(1, len(frame)))
    confidence = min(0.95, 0.35 + 0.35 * coverage_score + 0.25 * min(1.0, amplitude))
    if not both:
        confidence *= 0.65
    return {
        "sos": sos,
        "peak_date": peak_date,
        "eos": eos,
        "method": PHENOLOGY_VERSION if both else f"{PHENOLOGY_VERSION}-one-boundary-fallback",
        "confidence": round(float(confidence), 4),
        "qa_status": "detected" if both else "partial_boundary_fallback",
        "optical_valid_weeks": optical_valid,
        "sar_supported_weeks": sar_supported,
    }


def integrate_field_npp(
    npp: pd.DataFrame,
    sos: date,
    eos: date,
) -> dict[str, float | int]:
    total_days = max(1, (eos - sos).days + 1)
    seasonal_npp = 0.0
    valid_days = 0
    periods_used = 0
    weighted_coverage = 0.0
    weighted_effective_pixels = 0.0
    for row in npp.itertuples(index=False):
        start = pd.Timestamp(row.period_start).date()
        end = pd.Timestamp(row.period_end).date()
        days = overlap_days(start, end, sos, eos)
        if days == 0 or pd.isna(row.npp_rate_gC_m2_day):
            continue
        coverage = float(row.coverage_fraction) if not pd.isna(row.coverage_fraction) else 0.0
        seasonal_npp += float(row.npp_rate_gC_m2_day) * days
        valid_days += days
        periods_used += 1
        weighted_coverage += coverage * days
        effective = getattr(row, "effective_pixel_count", np.nan)
        if not pd.isna(effective):
            weighted_effective_pixels += float(effective) * days
    return {
        "seasonal_npp_gC_m2": seasonal_npp if valid_days else np.nan,
        "npp_valid_days": valid_days,
        "npp_total_days": total_days,
        "npp_temporal_coverage": valid_days / total_days,
        "npp_mean_spatial_coverage": weighted_coverage / valid_days if valid_days else np.nan,
        "npp_mean_effective_pixels": weighted_effective_pixels / valid_days if valid_days else np.nan,
        "npp_periods_used": periods_used,
    }
