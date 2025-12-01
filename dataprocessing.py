import pandas as pd
import numpy as np
from datetime import timedelta
from dateutil.easter import easter

df = pd.read_csv("cocoa.csv", parse_dates=["Date"])

df = df.sort_values("Date").reset_index(drop=True)

df = df.drop_duplicates()

df = df.drop(columns=["YEAR", "DOY","Unnamed: 0"], errors="ignore")

df["Price_NY"] = df["Price_NY"].replace(",", "", regex=True).astype(float)
df.loc[(df["Price_NY"] <= 0) | (df["Price_NY"] > 10000), "Price_NY"] = np.nan

df["Mid.Rate"] = df["Mid.Rate"].replace(",", "", regex=True).astype(float)
df.loc[(df["Mid.Rate"] <= 0) | (df["Mid.Rate"] > 1000), "Mid.Rate"] = np.nan

for col in ["T2M", "T2M_MAX", "T2M_MIN", "PRECTOTCORR", "ALLSKY_SFC_SW_DWN"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col] < 0, col] = np.nan

df = df.fillna(method="ffill")

df["MONTH"] = df["Date"].dt.month
df["WEEKDAY"] = df["Date"].dt.weekday + 1

df["logprice"] = np.log(df["Price_NY"])

df_clean = df.dropna()

years = df["Date"].dt.year.unique()

holiday_core_dates = []

for y in years:
    holiday_core_dates.append(pd.Timestamp(f"{y}-12-25")) 
    holiday_core_dates.append(pd.Timestamp(f"{y}-10-31"))  
    holiday_core_dates.append(easter(int(y)))          

holiday_extended_dates = set()
for d in holiday_core_dates:
    for offset in range(-5, 6): 
        holiday_extended_dates.add(d + timedelta(days=offset))

holiday_extended_dates = pd.to_datetime(list(holiday_extended_dates))

df["Is_Holiday"] = df["Date"].isin(holiday_extended_dates).astype(int)

df_clean = df.dropna()

trend_df = pd.read_csv("multiTimeline.csv", skiprows=2)

trend_df.columns = ["Month", "Cocoa_Trend"]

trend_df["Month"] = pd.to_datetime(trend_df["Month"], format="%Y-%m")

df["Month"] = df["Date"].dt.to_period("M").dt.to_timestamp()
df = df.merge(trend_df, on="Month", how="left")

df = df.drop(columns=["Month"], errors="ignore")

df = df.sort_values("Date")

df["Month"] = df["Date"].dt.to_period("M").dt.to_timestamp()

monthly_return = df.groupby("Month").apply(
    lambda x: np.log(x["Price_NY"].iloc[-1] / x["Price_NY"].iloc[0])
).reset_index(name="log_return_rate")

monthly_avg = df.groupby("Month").agg({
    "Mid.Rate": "mean",
    "T2M": "mean",
    "T2M_MAX": "mean",
    "T2M_MIN": "mean",
    "PRECTOTCORR": "mean",
    "ALLSKY_SFC_SW_DWN": "mean",
    "Cocoa_Trend": "mean",           
    "logprice": "mean",
    "Price_NY": "mean"  
}).reset_index()

monthly_avg = pd.merge(monthly_avg, monthly_return, on="Month", how="left")

holiday_monthly = df.groupby("Month")["Is_Holiday"].max().reset_index()
 
monthly_data = pd.merge(monthly_avg, holiday_monthly, on="Month", how="left")

monthly_data["YEAR"] = monthly_data["Month"].dt.year
monthly_data["MONTH_NUM"] = monthly_data["Month"].dt.month

monthly_data.to_csv("monthly_data.csv", index=False)