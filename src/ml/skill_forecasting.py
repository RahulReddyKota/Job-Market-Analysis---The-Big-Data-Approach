"""Forecast skill demand for next 6-12 months.

Reads gold monthly_skill_demand and writes forecasts to data/gold/skill_forecasts (Parquet).
Approach: per-skill time series with simple ETS-native replacement using sklearn-like pipeline:
- Resample monthly counts, fill missing months with 0
- Fit simple linear trend + seasonal month dummies
- Extrapolate 6 and 12 months ahead
"""
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path("data/gold/skill_forecasts")

def load_monthly() -> pd.DataFrame:
    p = Path("data/gold/monthly_skill_demand")
    if not p.exists():
        raise FileNotFoundError("monthly_skill_demand not found. Run Spark ETL.")
    df = pd.read_parquet(p)
    # Expect columns: year_month, skill, mentions
    if not {"year_month","skill","mentions"}.issubset(df.columns):
        raise ValueError("monthly_skill_demand missing required columns")
    # Parse date
    df = df.copy()
    df["year_month"] = pd.to_datetime(df["year_month"] + "-01", errors="coerce")
    df = df.dropna(subset=["year_month","skill"]) 
    return df

def forecast_skill(df_skill: pd.DataFrame, horizon: int = 12) -> pd.DataFrame:
    s = df_skill.set_index("year_month")["mentions"].sort_index()
    # Fill monthly gaps
    idx = pd.period_range(s.index.min().to_period('M'), s.index.max().to_period('M'), freq='M').to_timestamp()
    s = s.reindex(idx).fillna(0.0)
    # Build simple design matrix: time trend + month dummies
    t = np.arange(len(s))
    month = s.index.month
    X = pd.get_dummies(month.astype(int), prefix="m", drop_first=True)
    X["t"] = t
    y = s.values
    # Linear regression (ols)
    beta, *_ = np.linalg.lstsq(X.values, y, rcond=None)
    # Forecast horizon
    future_idx = pd.date_range(s.index[-1] + pd.offsets.MonthBegin(), periods=horizon, freq='MS')
    t_future = np.arange(len(s), len(s) + horizon)
    month_future = future_idx.month
    Xf = pd.get_dummies(month_future.astype(int), prefix="m", drop_first=True)
    # Align columns
    for c in X.columns:
        if c not in Xf.columns and c != "t":
            Xf[c] = 0
    for c in Xf.columns:
        if c not in X.columns and c != "t":
            X[c] = 0
    Xf = Xf[X.drop(columns=["t"]).columns]
    Xf["t"] = t_future
    y_pred = Xf.values @ beta
    out = pd.DataFrame({
        "date": future_idx,
        "forecast": y_pred
    })
    return out

def main():
    df = load_monthly()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    # Limit to most frequent skills to keep runtime reasonable
    top_skills = (
        df.groupby("skill")["mentions"].sum().sort_values(ascending=False).head(200).index
    )
    for skill in top_skills:
        sub = df[df["skill"] == skill][["year_month","skill","mentions"]]
        fc = forecast_skill(sub, horizon=12)
        fc["skill"] = skill
        results.append(fc)
    all_fc = pd.concat(results, ignore_index=True)
    all_fc.to_parquet(OUT_DIR, index=False)
    print(f"Wrote forecasts to {OUT_DIR}")

if __name__ == "__main__":
    main()







