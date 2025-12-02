import pandas as pd
import numpy as np
from scipy.stats import percentileofscore
import os

# ---------------- Helper Functions ----------------
def compute_date_range_mean(df, col, days, new_col_name):
    means = []
    for current_date in df.index:
        start = current_date - pd.Timedelta(days=days - 1)
        subset = df.loc[start:current_date, col]
        means.append(subset.mean())
    df[new_col_name] = means
    return df

def compute_date_range_zscore(df, col, days, new_col_name):
    zscores = []
    for current_date in df.index:
        start_date = current_date - pd.Timedelta(days=days - 1)
        subset = df.loc[start_date:current_date, col]
        if len(subset.dropna()) >= 5:
            mean = subset.mean()
            std = subset.std()
            z = (df.loc[current_date, col] - mean) / std if std != 0 else np.nan
        else:
            z = np.nan
        zscores.append(z)
    df[new_col_name] = zscores
    return df

def compute_1y_percentile(df, col, new_col_name):
    df[new_col_name] = df.apply(
        lambda row: 
        percentileofscore(
            df.loc[row.name - pd.DateOffset(years=1):row.name, col],
            row[col]
        ) / 100 if not pd.isna(row[col]) else np.nan,
        axis=1
    )
    return df

def add_streak_and_cumchange(df, col_name, streak_col_prefix="spread"):
    streak_days = []
    streak_cum_change = []
    streak = 0
    cum_change = 0
    prev = None
    start_value = None

    for val in df[col_name]:
        if prev is None or val == prev:
            streak = 0
            cum_change = 0
            start_value = val
        elif val > prev:
            if streak >= 0:
                streak += 1
                cum_change = val - start_value
            else:
                streak = 1
                start_value = prev
                cum_change = val - start_value
        elif val < prev:
            if streak <= 0:
                streak -= 1
                cum_change = val - start_value
            else:
                streak = -1
                start_value = prev
                cum_change = val - start_value
        prev = val
        streak_days.append(streak)
        streak_cum_change.append(round(cum_change, 2))

    df[f"{streak_col_prefix}_streak_days"] = streak_days
    df[f"{streak_col_prefix}_streak_cum_change"] = streak_cum_change
    return df

# ---------------- RRP ----------------
df_rrp = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=RRPONTSYD")
df_rrp = df_rrp.rename(columns={"observation_date": "date", "RRPONTSYD": "RRP_balance"})
df_rrp['date'] = pd.to_datetime(df_rrp['date'])
df_rrp = df_rrp.set_index('date').sort_index()
df_rrp = df_rrp.dropna(subset=['RRP_balance'])
df_rrp['RRP_change_prev_point'] = df_rrp['RRP_balance'] - df_rrp['RRP_balance'].shift(1)
df_rrp['RRP_pectage_change_prev_point'] = df_rrp['RRP_change_prev_point']/df_rrp['RRP_balance'].shift(1)
df_rrp = compute_date_range_mean(df_rrp, 'RRP_balance', 7, 'RRP_ma_7d')
df_rrp = compute_date_range_mean(df_rrp, 'RRP_balance', 14, 'RRP_ma_14d')
df_rrp = compute_date_range_mean(df_rrp, 'RRP_balance', 30, 'RRP_ma_30d')
df_rrp = compute_1y_percentile(df_rrp, 'RRP_balance', 'RRP_percentile_1y')
df_rrp = df_rrp.tail(1500)

# ---------------- SRF ----------------
df_srf = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=RPONTSYD")
df_srf = df_srf.rename(columns={"observation_date": "date", "RPONTSYD": "SRF_balance"})
df_srf['date'] = pd.to_datetime(df_srf['date'])
df_srf = df_srf.set_index('date').sort_index()
df_srf = df_srf.dropna(subset=['SRF_balance'])
df_srf['SRF_change_prev_point'] = df_srf['SRF_balance'] - df_srf['SRF_balance'].shift(1)
df_srf = compute_date_range_mean(df_srf, 'SRF_balance', 7, 'SRF_ma_7d')
df_srf = compute_date_range_mean(df_srf, 'SRF_balance', 14, 'SRF_ma_14d')
df_srf = compute_date_range_mean(df_srf, 'SRF_balance', 30, 'SRF_ma_30d')
df_srf = compute_1y_percentile(df_srf, 'SRF_balance', 'SRF_percentile_1y')
df_srf = df_srf.tail(1500)

# ---------------- WALCL & TGA ----------------
df_walcl = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=RESPPANWW")
df_walcl = df_walcl.rename(columns={'observation_date': 'date', 'RESPPANWW': 'walcl'})
df_tga = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=WTREGEN")
df_tga = df_tga.rename(columns={'observation_date': 'date', 'WTREGEN': 'tga'})
df_walcl['date'] = pd.to_datetime(df_walcl['date'])
df_tga['date'] = pd.to_datetime(df_tga['date'])
df_walcl = df_walcl.set_index('date').sort_index().tail(400)
df_tga = df_tga.set_index('date').sort_index().tail(400)

# ---------------- IG ----------------
df_ig = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A0CM")
df_ig = df_ig.rename(columns={'observation_date': 'date', 'BAMLC0A0CM': 'IG_OAS'})
df_ig['date'] = pd.to_datetime(df_ig['date'])
df_ig = df_ig.set_index('date').sort_index().tail(150)
df_ig['IG_OAS_Δ1d'] = df_ig['IG_OAS'].diff()
df_ig['IG_OAS_Δ7d'] = df_ig['IG_OAS'].diff(7)
df_ig = compute_date_range_mean(df_ig, 'IG_OAS', 7, 'IG_OAS_MA7d')
df_ig = compute_date_range_mean(df_ig, 'IG_OAS', 30, 'IG_OAS_MA30d')
df_ig = compute_1y_percentile(df_ig, 'IG_OAS', 'IG_OAS_percentile_1y')
df_ig = compute_date_range_zscore(df_ig, 'IG_OAS', 90, 'IG_OAS_zscore_3m')
df_ig = add_streak_and_cumchange(df_ig, "IG_OAS", streak_col_prefix="IG_OAS").tail(60)

# ---------------- HY ----------------
df_hy = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2")
df_hy = df_hy.rename(columns={'observation_date': 'date', 'BAMLH0A0HYM2': 'HY_OAS'})
df_hy['date'] = pd.to_datetime(df_hy['date'])
df_hy = df_hy.set_index('date').sort_index().tail(150)
df_hy['change_1d'] = df_hy['HY_OAS'].diff()
df_hy['change_7d'] = df_hy['HY_OAS'].diff(7)
df_hy = compute_date_range_mean(df_hy, 'HY_OAS', 7, 'HY_OAS_ma_7d')
df_hy = compute_date_range_mean(df_hy, 'HY_OAS', 30, 'HY_OAS_ma_30d')
df_hy = compute_1y_percentile(df_hy, 'HY_OAS', 'HY_OAS_percentile_1y')
df_hy = compute_date_range_zscore(df_hy, 'HY_OAS', 90, 'HY_OAS_zscore_3m')
df_hy = add_streak_and_cumchange(df_hy, 'HY_OAS', streak_col_prefix='HY_OAS').tail(60)

# ---------------- SOFR-EFFR ----------------
df_sofr = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=SOFR")
df_effr = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=EFFR")
df_sofr = df_sofr.rename(columns={'observation_date': 'date'})
df_effr = df_effr.rename(columns={'observation_date': 'date'})
df_sofr['date'] = pd.to_datetime(df_sofr['date'])
df_effr['date'] = pd.to_datetime(df_effr['date'])
df_sofr = df_sofr.set_index('date').sort_index()
df_effr = df_effr.set_index('date').sort_index()
df_se = pd.concat([df_sofr, df_effr], axis=1).dropna()
df_se['SOFR-EFFR'] = df_se['SOFR'] - df_se['EFFR']
df_se = df_se.drop(columns=['SOFR', 'EFFR'])
df_se = add_streak_and_cumchange(df_se, 'SOFR-EFFR', streak_col_prefix='SOFR-EFFR').tail(60)

# ---------------- Stress Index ----------------
df_stress = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=STLFSI4")
df_stress = df_stress.rename(columns={'observation_date': 'date', 'STLFSI4': 'stress_index'})
df_stress['date'] = pd.to_datetime(df_stress['date'])
df_stress = df_stress.set_index('date').sort_index()
df_stress = compute_date_range_zscore(df_stress, 'stress_index', 180, 'stress_index_zscore_6m')
df_stress = df_stress.tail(40)

# ---------------- Export ----------------
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_updated.xlsx")
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_rrp.to_excel(writer, sheet_name="rrp")
    df_srf.to_excel(writer, sheet_name="srf")
    df_walcl.to_excel(writer, sheet_name="walcl")
    df_tga.to_excel(writer, sheet_name="tga")
    df_ig.to_excel(writer, sheet_name="ig")
    df_hy.to_excel(writer, sheet_name="hy")
    df_se.to_excel(writer, sheet_name="se")
    df_stress.to_excel(writer, sheet_name="stress")
print(f"✅ 已导出到: {output_path}")
