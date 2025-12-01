import pandas as pd
import requests
import numpy as np
from scipy.stats import zscore

from scipy.stats import percentileofscore
#Daily data for 资金供应端趋势
url_rrp = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=RRPONTSYD"
df_rrp = pd.read_csv(url_rrp)
df_rrp = df_rrp.rename(columns={
    'observation_date': 'date',
    'RRPONTSYD': 'RRP_balance'
})
# 将 date 转为 datetime 类型并设为 index
df_rrp['date'] = pd.to_datetime(df_rrp['date'])
df_rrp = df_rrp.set_index('date')

# 1. 昨日值
df_rrp['RRP_yesterday'] = df_rrp['RRP_balance'].shift(1)

# 2. 单日变化值（绝对值）
df_rrp['RRP_daily_change'] = df_rrp['RRP_balance'] - df_rrp['RRP_yesterday']

# 3. 单日变化率（百分比）
df_rrp['RRP_daily_pct_change'] = df_rrp['RRP_daily_change'] / df_rrp['RRP_yesterday']

# 4. 移动均值（7日、14日、30日）
df_rrp['RRP_ma_7'] = df_rrp['RRP_balance'].rolling(7,min_periods=5).mean()
df_rrp['RRP_ma_14'] = df_rrp['RRP_balance'].rolling(14, min_periods=12).mean()
df_rrp['RRP_ma_30'] = df_rrp['RRP_balance'].rolling(30,min_periods=25).mean()

# 5. 移动标准差（波动性判断用）
df_rrp['RRP_std_7'] = df_rrp['RRP_balance'].rolling(7,min_periods=5).std()

# 6. Z-score（统计异常识别）
df_rrp['RRP_zscore_30'] = (df_rrp['RRP_balance'] - df_rrp['RRP_ma_30']) / df_rrp['RRP_balance'].rolling(30,min_periods=25).std()

# 7. 分位数位置（过去1年或可调）
window = 252  # 约一年的交易日
df_rrp['RRP_percentile_1y'] = df_rrp['RRP_balance'].rolling(window,min_periods=230).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1])
df_rrp= df_rrp.dropna(subset=['RRP_balance'])
df_rrp= df_rrp.drop(columns='RRP_yesterday')
df_rrp = df_rrp.tail(1500)
#print(df_rrp)
url_srf = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=RPONTSYD"
df_srf = pd.read_csv(url_srf)
df_srf = df_srf.rename(columns={
    'observation_date': 'date',         # 若 FRED csv 用的是 "DATE"
    'RPONTSYD': 'SRF_balance'    # 自定义 name: SRF_rate
})

# 把 date 转成 datetime，并设为 index
df_srf['date'] = pd.to_datetime(df_srf['date'])
df_srf = df_srf.set_index('date').sort_index()
df_srf['SRF_yesterday'] = df_srf['SRF_balance'].shift(1)

# 2. 单日变化值（绝对值）
df_srf['SRF_daily_change'] = df_srf['SRF_balance'] - df_srf['SRF_yesterday']

# 3. 单日变化率（百分比）
#df_srf['SRF_daily_pct_change'] = df_srf['SRF_daily_change'] / df_srf['SRF_yesterday']

# 4. 移动均值（7日、14日、30日）
df_srf['SRF_ma_7'] = df_srf['SRF_balance'].rolling(7, min_periods=5).mean()
df_srf['SRF_ma_14'] = df_srf['SRF_balance'].rolling(14, min_periods=12).mean()
df_srf['SRF_ma_30'] = df_srf['SRF_balance'].rolling(30, min_periods=25).mean()

# 5. 移动标准差（波动性判断用）
df_srf['SRF_std_7'] = df_srf['SRF_balance'].rolling(7, min_periods=5).std()

# 6. Z-score（统计异常识别）
df_srf['SRF_zscore_30'] = (df_srf['SRF_balance'] - df_srf['SRF_ma_30']) / df_srf['SRF_balance'].rolling(30, min_periods=25).std()

# 7. 分位数位置（过去1年或可调）
window = 252  # 约一年的交易日
df_srf['SRF_percentile_1y'] = df_srf['SRF_balance'].rolling(window, min_periods=230).apply(
    lambda x: pd.Series(x).rank(pct=True).iloc[-1]
)

# 8. 清洗：去除 SRF_balance 为 NaN 的行
df_srf = df_srf.dropna(subset=['SRF_balance'])

# 9. 可选：删除昨日值列（如果不再需要）
df_srf = df_srf.drop(columns='SRF_yesterday')
df_srf = df_srf.tail(1500)
#Weekly data for 资金供应端趋势
url_walcl = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=RESPPANWW"
df_walcl = pd.read_csv(url_walcl)

# 重命名列
df_walcl = df_walcl.rename(columns={
    'observation_date': 'date',         
    'RESPPANWW': 'walcl'    
})
url_tga = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=WTREGEN"
df_tga = pd.read_csv(url_tga)

# 重命名列
df_tga = df_tga.rename(columns={
    'observation_date': 'date',
    'WTREGEN': 'tga'   
})
# 转换 date 并设为 index
df_tga['date'] = pd.to_datetime(df_tga['date'])
df_tga = df_tga.set_index('date').sort_index()
df_walcl['date'] = pd.to_datetime(df_walcl['date'])
df_walcl = df_walcl.set_index('date').sort_index()
df_week_1 = pd.concat([df_walcl, df_tga], axis=1, join='outer')
df_week_1 = df_week_1.tail(400)
url_ig = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A0CM"
df_ig = pd.read_csv(url_ig)
df_ig = df_ig.rename(columns={
    'observation_date': 'date',
    'BAMLC0A0CM': 'IG_OAS'   
})
df_ig['date'] = pd.to_datetime(df_ig['date'])
df_ig = df_ig.set_index('date').sort_index()


df_ig = df_ig.copy()
df_ig.sort_index(inplace=True)  # 确保按时间排序
df_ig = df_ig[-150:]
# 单日/周变化
df_ig['IG_OAS_Δ1d'] = df_ig['IG_OAS'].diff()
df_ig['IG_OAS_Δ7d'] = df_ig['IG_OAS'].diff(7)

# 移动均值
df_ig['IG_OAS_MA7'] = df_ig['IG_OAS'].rolling(7, min_periods=5).mean()
df_ig['IG_OAS_MA30'] = df_ig['IG_OAS'].rolling(30, min_periods=25).mean()
df_ig['IG_OAS_vs_MA7_diff'] = df_ig['IG_OAS'] - df_ig['IG_OAS_MA7']

# Z-score (rolling)
df_ig['IG_OAS_zscore_30d'] = (
    (df_ig['IG_OAS'] - df_ig['IG_OAS'].rolling(30).mean()) /
    df_ig['IG_OAS'].rolling(30).std()
)

# 分位数（rolling 计算）
def rolling_percentile(series, window):
    return series.rolling(window).apply(lambda x: percentileofscore(x, x.iloc[-1]) / 100, raw=False)

df_ig['IG_OAS_pctile_30d'] = rolling_percentile(df_ig['IG_OAS'], 30)
import pandas as pd
import numpy as np

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
        streak_cum_change.append(round(cum_change , 2))  # 单位：bp

    df[f"{streak_col_prefix}_streak_days"] = streak_days
    df[f"{streak_col_prefix}_streak_cum_change"] = streak_cum_change
    return df


df_ig = add_streak_and_cumchange(df_ig, "IG_OAS", streak_col_prefix="IG_OAS")
#df_hy = add_streak_and_cumchange(df_hy, "HY_OAS", streak_col_prefix="HY_OAS")
df_ig = df_ig.tail(60)
url_hy = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2"
df_hy = pd.read_csv(url_hy)

df_hy = df_hy.rename(columns={
    'observation_date': 'date',
    'BAMLH0A0HYM2': 'HY_OAS'  
})
df_hy['date'] = pd.to_datetime(df_hy['date'])
df_hy = df_hy.set_index('date').sort_index()

df_hy = df_hy.tail(150)                      # 原始数据：高收益利差
df_hy["change_1d"]                   = df_hy["HY_OAS"].diff(1)     # 单日变化
df_hy["change_7d"]                   = df_hy["HY_OAS"].diff(7)     # 周度变化
df_hy["ma_7d"]                       = df_hy["HY_OAS"].rolling(7).mean()     # 7日均值
df_hy["ma_30d"]                      = df_hy["HY_OAS"].rolling(30).mean()    # 30日均值
df_hy["zscore_30d"]                  = (df_hy["HY_OAS"] - df_hy["HY_OAS"].rolling(30).mean()) / df_hy["HY_OAS"].rolling(30).std()  # Z-score
df_hy["percentile_30d"]              = df_hy["HY_OAS"].rolling(30).apply(lambda x: (x.rank().iloc[-1] - 1) / (len(x)-1), raw=False)  # 30日分位
df_hy = add_streak_and_cumchange(df_hy, "HY_OAS", streak_col_prefix="HY_OAS")
df_hy = df_hy.tail(60)

url_sofr = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SOFR"
df_sofr = pd.read_csv(url_sofr)
df_sofr = df_sofr.rename(columns={
    'observation_date': 'date' 
})
url_effr = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=effr"
df_effr = pd.read_csv(url_effr)
df_effr = df_effr.rename(columns={
    'observation_date': 'date' 
})
df_sofr['date'] = pd.to_datetime(df_sofr['date'])
df_sofr = df_sofr.set_index('date').sort_index()
df_effr['date'] = pd.to_datetime(df_effr['date'])
df_effr = df_effr.set_index('date').sort_index()
df_se= pd.concat([df_sofr, df_effr], axis=1, join='outer')
df_se= df_se.dropna(how='any')
df_se["SOFR-EFFR"] = df_se['SOFR']-df_se['EFFR']
df_se = df_se.drop(columns=["SOFR", "EFFR"])
df_se = add_streak_and_cumchange(df_se, "SOFR-EFFR", streak_col_prefix="SOFR-EFFR")
df_se = df_se.tail(60)
url_stress = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=STLFSI4"
df_stress = pd.read_csv(url_stress)
df_stress = df_stress.rename(columns={
    'observation_date': 'date' ,
    'STLFSI4':'stress_index'
})

df_stress['date'] = pd.to_datetime(df_stress['date'])
df_stress = df_stress.set_index('date').sort_index()
df_stress = df_stress.tail(40)
import os

output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output.xlsx")

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_rrp.to_excel(writer, sheet_name="rrp", index=True)
    df_srf.to_excel(writer, sheet_name="srf", index=True)
    df_walcl.to_excel(writer, sheet_name="walcl", index=True)
    df_tga.to_excel(writer, sheet_name="tga", index=True)
    df_ig.to_excel(writer, sheet_name="ig", index=True)
    df_hy.to_excel(writer, sheet_name="hy", index=True)
    df_se.to_excel(writer, sheet_name="se", index=True)
    df_stress.to_excel(writer, sheet_name="stress", index=True)
from google.cloud import storage
import os

def upload_to_gcs(bucket_name, local_file_path, gcs_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"✅ 上传成功：gs://{bucket_name}/{gcs_blob_name}")

# 上传到 GCS
upload_to_gcs(
    bucket_name="angel-project",            # 你的 bucket 名字
    local_file_path=output_path,            # 你之前定义好的 output.xlsx 路径
    gcs_blob_name="reports/output.xlsx"     # 上传到 bucket 下的哪个路径/文件名
)

