from __future__ import annotations

import calendar
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd


METHOD_VERSION = "wapor-npp-literature-v1.1"
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


def _normalize_matrix(values: np.ndarray) -> np.ndarray:
    """Robust row-wise normalization used by the production phenology path."""
    values = np.asarray(values, dtype=np.float32)
    output = np.full(values.shape, np.nan, dtype=np.float32)
    counts = np.isfinite(values).sum(axis=1)
    eligible = counts >= 3
    if not eligible.any():
        return output
    subset = values[eligible]
    with np.errstate(all="ignore"):
        low = np.nanquantile(subset, 0.1, axis=1)
        high = np.nanquantile(subset, 0.9, axis=1)
    span = high - low
    usable = np.isfinite(span) & (span > 1e-8)
    normalized = np.full(subset.shape, np.nan, dtype=np.float32)
    normalized[usable] = np.clip(
        (subset[usable] - low[usable, None]) / span[usable, None], 0, 1
    )
    output[eligible] = normalized
    return output


def _interpolate_short_gaps(values: np.ndarray, limit: int = 2) -> np.ndarray:
    values = np.asarray(values, dtype=np.float32)
    result = values.copy()
    valid = np.isfinite(values)
    width = values.shape[1]
    positions = np.arange(width, dtype=np.int16)[None, :]
    previous = np.maximum.accumulate(np.where(valid, positions, -1), axis=1)
    following = np.minimum.accumulate(
        np.where(valid, positions, width)[:, ::-1], axis=1
    )[:, ::-1]
    missing = ~valid

    interior = missing & (previous >= 0) & (following < width)
    gap = following - previous - 1
    interior &= gap <= limit
    rows, cols = np.where(interior)
    if len(rows):
        left = values[rows, previous[rows, cols]]
        right = values[rows, following[rows, cols]]
        fraction = (cols - previous[rows, cols]) / (
            following[rows, cols] - previous[rows, cols]
        )
        result[rows, cols] = left + (right - left) * fraction

    leading = missing & (previous < 0) & (following < width) & (following <= limit)
    rows, cols = np.where(leading)
    if len(rows):
        result[rows, cols] = values[rows, following[rows, cols]]
    trailing = missing & (previous >= 0) & (following >= width) & ((positions - previous) <= limit)
    rows, cols = np.where(trailing)
    if len(rows):
        result[rows, cols] = values[rows, previous[rows, cols]]
    return result


def derive_phenology_matrix(
    field_ids: np.ndarray,
    week_dates: np.ndarray,
    ndvi: np.ndarray,
    evi: np.ndarray,
    lai: np.ndarray,
    rvi: np.ndarray,
    season_start: date,
    season_end: date,
    *,
    threshold_fraction: float = 0.20,
    minimum_optical_weeks: int = 6,
) -> pd.DataFrame:
    """Vectorized production equivalent of :func:`derive_phenology`.

    Arrays are shaped ``(fields, weeks)`` and share one ordered weekly axis.
    The result retains the conservative full-season and one-boundary fallbacks
    while avoiding one Python/Pandas object per production field.
    """
    field_ids = np.asarray(field_ids)
    week_dates = np.asarray(week_dates, dtype="datetime64[D]")
    matrices = [np.asarray(value, dtype=np.float32) for value in (ndvi, evi, lai, rvi)]
    if not matrices or any(value.shape != matrices[0].shape for value in matrices):
        raise ValueError("Phenology matrices must have the same shape")
    if matrices[0].shape != (len(field_ids), len(week_dates)):
        raise ValueError("Phenology matrix axes do not match field IDs and dates")

    optical_stack = np.stack([_normalize_matrix(value) for value in matrices[:3]], axis=2)
    with np.errstate(all="ignore"):
        optical = np.nanmedian(optical_stack, axis=2).astype(np.float32)
    optical[np.all(~np.isfinite(optical_stack), axis=2)] = np.nan
    optical_valid = np.isfinite(optical).sum(axis=1)
    rvi_normalized = _normalize_matrix(matrices[3])
    sar_support = ((~np.isfinite(optical)) & np.isfinite(rvi_normalized)).sum(axis=1)
    signal = np.where(np.isfinite(optical), optical, rvi_normalized)
    signal = _interpolate_short_gaps(signal, limit=2)
    padded = np.pad(signal, ((0, 0), (1, 1)), constant_values=np.nan)
    rolling_stack = np.stack(
        [padded[:, :-2], padded[:, 1:-1], padded[:, 2:]], axis=2
    )
    with np.errstate(all="ignore"):
        smoothed = np.nanmedian(rolling_stack, axis=2).astype(np.float32)
    smoothed[np.all(~np.isfinite(rolling_stack), axis=2)] = np.nan
    valid = np.isfinite(smoothed)
    valid_count = valid.sum(axis=1)

    safe = np.where(valid, smoothed, -np.inf)
    peak_index = np.argmax(safe, axis=1)
    has_signal = valid_count > 0
    peak_value = safe[np.arange(len(field_ids)), peak_index]
    forward_rank = np.cumsum(valid, axis=1)
    reverse_rank = np.cumsum(valid[:, ::-1], axis=1)[:, ::-1]
    edge_mask = valid & ((forward_rank <= 3) | (reverse_rank <= 3))
    with np.errstate(all="ignore"):
        baseline = np.nanmedian(np.where(edge_mask, smoothed, np.nan), axis=1)
    amplitude = peak_value - baseline
    threshold = baseline + threshold_fraction * amplitude

    columns = np.arange(len(week_dates))[None, :]
    above = valid & (smoothed >= threshold[:, None]) & (columns <= peak_index[:, None])
    below = valid & (smoothed <= threshold[:, None]) & (columns >= peak_index[:, None])
    above_pairs = above[:, :-1] & above[:, 1:]
    below_pairs = below[:, :-1] & below[:, 1:]
    has_sos = above_pairs.any(axis=1)
    has_eos = below_pairs.any(axis=1)
    sos_index = np.argmax(above_pairs, axis=1)
    eos_index = np.argmax(below_pairs, axis=1)
    both = has_sos & has_eos

    start_value = np.datetime64(season_start)
    end_value = np.datetime64(season_end)
    sos = np.full(len(field_ids), start_value, dtype="datetime64[D]")
    eos = np.full(len(field_ids), end_value, dtype="datetime64[D]")
    sos[has_sos] = week_dates[sos_index[has_sos]]
    eos[has_eos] = week_dates[eos_index[has_eos]]
    peak_date = np.full(len(field_ids), "NaT", dtype="datetime64[D]")
    peak_date[has_signal] = week_dates[peak_index[has_signal]]
    invalid_order = has_signal & (
        (sos > peak_date) | (eos < peak_date) | (sos >= eos)
    )
    sos[invalid_order] = start_value
    eos[invalid_order] = end_value
    has_sos[invalid_order] = False
    has_eos[invalid_order] = False
    both = has_sos & has_eos

    insufficient = (optical_valid < minimum_optical_weeks) | (valid_count < minimum_optical_weeks)
    weak = (~insufficient) & (~np.isfinite(amplitude) | (amplitude < 0.20))
    detected = (~insufficient) & (~weak)
    sos[insufficient | weak] = start_value
    eos[insufficient | weak] = end_value

    qa = np.full(len(field_ids), "partial_boundary_fallback", dtype=object)
    qa[detected & both] = "detected"
    qa[insufficient] = "insufficient_optical_weeks"
    qa[insufficient & ~has_signal] = "no_weekly_signal"
    qa[weak] = "weak_seasonal_amplitude"
    method = np.full(
        len(field_ids), f"{PHENOLOGY_VERSION}-matrix-one-boundary-fallback", dtype=object
    )
    method[detected & both] = f"{PHENOLOGY_VERSION}-matrix"
    method[insufficient | weak] = "full-season-fallback"

    coverage_score = np.minimum(1.0, optical_valid / max(1, len(week_dates)))
    confidence = np.minimum(0.95, 0.35 + 0.35 * coverage_score + 0.25 * np.minimum(1.0, amplitude))
    confidence[detected & ~both] *= 0.65
    confidence[weak] = 0.2
    confidence[insufficient] = np.where(has_signal[insufficient], 0.15, 0.0)

    return pd.DataFrame(
        {
            "field_id": field_ids,
            "sos": pd.to_datetime(sos),
            "peak_date": pd.to_datetime(peak_date),
            "eos": pd.to_datetime(eos),
            "method": method,
            "confidence": np.round(confidence.astype(float), 4),
            "qa_status": qa,
            "optical_valid_weeks": optical_valid.astype(np.int16),
            "sar_supported_weeks": sar_support.astype(np.int16),
        }
    )


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
