#!/usr/bin/env python
# coding: utf-8

# ## Ramesh_StockMarket_DataPipeline
# 
# null

# In[2]:


print("Stock Market Data Pipeline")
print("Lakehouse connection test started")

print("Spark version:", spark.version)

tables = spark.sql("SHOW TABLES").collect()

print("\nAvailable Lakehouse tables:")

if len(tables) == 0:
    print("No tables found - ready for a fresh project.")
else:
    for table in tables:
        print(table.tableName)

print("\nLakehouse connection successful.")


# In[3]:


# ============================================================
# STEP 2 — ALPHA VANTAGE CONFIGURATION
# ============================================================

API_KEY = "C3JXH6YRYNM0URHP"

BASE_URL = "https://www.alphavantage.co/query"

SYMBOLS = [
    "IBM",
    "AAPL",
    "JPM",
    "AMZN",
    "KO"
]

print("Alpha Vantage configuration completed")
print("Companies:", SYMBOLS)
print("Company count:", len(SYMBOLS))


# In[4]:


# ============================================================
# STEP 3 — ALPHA VANTAGE API TEST
# ============================================================

import requests

test_symbol = "IBM"

params = {
    "function": "TIME_SERIES_DAILY",
    "symbol": test_symbol,
    "outputsize": "compact",
    "apikey": API_KEY
}

response = requests.get(
    BASE_URL,
    params=params,
    timeout=30
)

print("HTTP Status:", response.status_code)

data = response.json()

# Check API response
if "Error Message" in data:
    raise Exception(
        "Alpha Vantage API error: " + data["Error Message"]
    )

if "Note" in data:
    raise Exception(
        "Alpha Vantage API limit message: " + data["Note"]
    )

if "Time Series (Daily)" not in data:
    raise Exception(
        "Unexpected API response. Available keys: "
        + str(list(data.keys()))
    )

time_series = data["Time Series (Daily)"]

print("API connection successful")
print("Company:", test_symbol)
print("Records received:", len(time_series))

print("\nFirst 5 records:")

for date, values in list(time_series.items())[:5]:
    print(date, values)


# In[5]:


# ============================================================
# STEP 4 — REUSABLE STOCK DATA EXTRACTION FUNCTION
# ============================================================

def get_stock_data(symbol):

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=30
    )

    if response.status_code != 200:
        raise Exception(
            f"{symbol}: HTTP error {response.status_code}"
        )

    data = response.json()

    if "Error Message" in data:
        raise Exception(
            f"{symbol}: {data['Error Message']}"
        )

    if "Note" in data:
        raise Exception(
            f"{symbol}: API limit reached. {data['Note']}"
        )

    if "Time Series (Daily)" not in data:
        raise Exception(
            f"{symbol}: Unexpected API response. "
            f"Available keys: {list(data.keys())}"
        )

    time_series = data["Time Series (Daily)"]

    records = []

    for date, values in time_series.items():

        records.append({
            "Date": date,
            "Open": float(values["1. open"]),
            "High": float(values["2. high"]),
            "Low": float(values["3. low"]),
            "Close": float(values["4. close"]),
            "Volume": int(values["5. volume"]),
            "Symbol": symbol
        })

    df = pd.DataFrame(records)

    # Convert Date to proper datetime
    df["Date"] = pd.to_datetime(df["Date"])

    # Sort oldest to newest
    df = df.sort_values("Date").reset_index(drop=True)

    # Calculate previous closing price
    df["Previous_Close"] = df["Close"].shift(1)

    # Calculate daily percentage change
    df["Daily_Change_Pct"] = (
        (df["Close"] - df["Previous_Close"])
        / df["Previous_Close"]
    ) * 100

    # Remove first row because it has no previous day
    df = df.dropna(subset=["Previous_Close"]).reset_index(drop=True)

    return df


print("Stock data extraction function created successfully")


# In[6]:


# ============================================================
# STEP 5 — TEST STOCK DATA EXTRACTION
# ============================================================

import pandas as pd

ibm_df = get_stock_data("IBM")

print("IBM data extracted successfully")
print("Rows:", len(ibm_df))
print("Columns:", list(ibm_df.columns))

print("\nData types:")
print(ibm_df.dtypes)

print("\nFirst 5 rows:")
display(ibm_df.head())


# In[7]:


# ============================================================
# STEP 6 — DATA QUALITY VALIDATION
# ============================================================

print("IBM DATA QUALITY CHECK")
print("=" * 50)

# Missing values
missing_values = ibm_df.isnull().sum()

print("\nMissing values:")
print(missing_values)

# Duplicate records
duplicate_count = ibm_df.duplicated(
    subset=["Symbol", "Date"]
).sum()

print("\nDuplicate Symbol + Date records:", duplicate_count)

# Invalid prices
invalid_prices = ibm_df[
    (ibm_df["Open"] <= 0) |
    (ibm_df["High"] <= 0) |
    (ibm_df["Low"] <= 0) |
    (ibm_df["Close"] <= 0)
]

print("Invalid price records:", len(invalid_prices))

# Invalid volume
invalid_volume = ibm_df[
    ibm_df["Volume"] < 0
]

print("Invalid volume records:", len(invalid_volume))

# Date range
print(
    "\nDate range:",
    ibm_df["Date"].min().date(),
    "to",
    ibm_df["Date"].max().date()
)

# Final result
if (
    missing_values.sum() == 0
    and duplicate_count == 0
    and len(invalid_prices) == 0
    and len(invalid_volume) == 0
):
    print("\n✅ IBM DATA QUALITY CHECK PASSED")
else:
    print("\n❌ IBM DATA QUALITY CHECK FAILED")


# In[8]:


# ============================================================
# STEP 7 — EXTRACT DATA FOR ALL COMPANIES
# ============================================================

import time

all_stock_data = []

for symbol in SYMBOLS:

    print(f"Fetching data for {symbol}...")

    try:
        company_df = get_stock_data(symbol)

        print(f"{symbol}: {len(company_df)} records extracted")

        all_stock_data.append(company_df)

    except Exception as e:
        print(f"{symbol}: ERROR - {e}")
        raise

    if symbol != SYMBOLS[-1]:
        print("Waiting 3 seconds before next request...")
        time.sleep(3)

# Combine all companies
combined_df = pd.concat(
    all_stock_data,
    ignore_index=True
)

print("\n==============================================")
print("ALL COMPANIES EXTRACTED SUCCESSFULLY")
print("==============================================")

print("Total rows:", len(combined_df))
print("Companies:", sorted(combined_df["Symbol"].unique()))

print("\nRows per company:")
print(
    combined_df["Symbol"]
    .value_counts()
    .sort_index()
)


# In[9]:


# ============================================================
# STEP 8 — COMBINED DATA QUALITY VALIDATION
# ============================================================

print("COMBINED DATA QUALITY CHECK")
print("=" * 60)

# 1. Missing values
missing_values = combined_df.isnull().sum()

print("\nMissing values:")
print(missing_values)

# 2. Duplicate company + date records
duplicate_count = combined_df.duplicated(
    subset=["Symbol", "Date"]
).sum()

print("\nDuplicate Symbol + Date records:", duplicate_count)

# 3. Invalid prices
invalid_prices = combined_df[
    (combined_df["Open"] <= 0) |
    (combined_df["High"] <= 0) |
    (combined_df["Low"] <= 0) |
    (combined_df["Close"] <= 0)
]

print("Invalid price records:", len(invalid_prices))

# 4. Invalid OHLC relationships
invalid_ohlc = combined_df[
    (combined_df["High"] < combined_df["Low"]) |
    (combined_df["High"] < combined_df["Open"]) |
    (combined_df["High"] < combined_df["Close"]) |
    (combined_df["Low"] > combined_df["Open"]) |
    (combined_df["Low"] > combined_df["Close"])
]

print("Invalid OHLC records:", len(invalid_ohlc))

# 5. Invalid volume
invalid_volume = combined_df[
    combined_df["Volume"] < 0
]

print("Invalid volume records:", len(invalid_volume))

# 6. Company count
company_count = combined_df["Symbol"].nunique()

print("\nCompany count:", company_count)

# 7. Total rows
print("Total rows:", len(combined_df))

# 8. Date range
print(
    "Date range:",
    combined_df["Date"].min().date(),
    "to",
    combined_df["Date"].max().date()
)

# 9. Final validation
if (
    missing_values.sum() == 0
    and duplicate_count == 0
    and len(invalid_prices) == 0
    and len(invalid_ohlc) == 0
    and len(invalid_volume) == 0
    and company_count == len(SYMBOLS)
):
    print("\n✅ COMBINED DATA QUALITY CHECK PASSED")
else:
    print("\n❌ COMBINED DATA QUALITY CHECK FAILED")


# In[10]:


# ============================================================
# STEP 9 — CREATE MAIN STOCK PRICES TABLE
# ============================================================

# Convert pandas DataFrame to Spark DataFrame
stock_prices_spark = spark.createDataFrame(combined_df)

# Write the validated data to the Lakehouse
stock_prices_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("stock_prices")

print("stock_prices table created successfully")

# Verify the table
stock_prices_check = spark.sql("""
    SELECT
        COUNT(*) AS Total_Rows,
        COUNT(DISTINCT Symbol) AS Total_Companies,
        MIN(Date) AS Earliest_Date,
        MAX(Date) AS Latest_Date
    FROM stock_prices
""")

stock_prices_check.show()


# In[11]:


# ============================================================
# STEP 10 — VERIFY STOCK_PRICES TABLE
# ============================================================

print("STOCK_PRICES TABLE STRUCTURE")
print("=" * 60)

spark.sql("""
    DESCRIBE stock_prices
""").show(truncate=False)

print("\nROW COUNT:")
spark.sql("""
    SELECT COUNT(*) AS Row_Count
    FROM stock_prices
""").show()

print("\nSAMPLE DATA:")
spark.sql("""
    SELECT *
    FROM stock_prices
    ORDER BY Date DESC, Symbol
    LIMIT 10
""").show(truncate=False)


# In[12]:


# ============================================================
# STEP 11 — CREATE COMPANY DIMENSION
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType

company_data = [
    ("AAPL", "Apple", "Technology"),
    ("AMZN", "Amazon", "E-commerce"),
    ("IBM", "IBM", "Technology"),
    ("JPM", "JPMorgan Chase", "Banking"),
    ("KO", "Coca-Cola", "Consumer Goods")
]

company_schema = StructType([
    StructField("Symbol", StringType(), False),
    StructField("Company_Name", StringType(), False),
    StructField("Sector", StringType(), False)
])

dim_company_df = spark.createDataFrame(
    company_data,
    company_schema
)

dim_company_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_company")

print("dim_company table created successfully")

spark.sql("""
    SELECT *
    FROM dim_company
    ORDER BY Symbol
""").show(truncate=False)


# In[13]:


# ============================================================
# STEP 12 — CREATE DATE DIMENSION
# ============================================================

from pyspark.sql import functions as F

# Get the minimum and maximum dates from stock_prices
date_range = spark.sql("""
    SELECT
        MIN(Date) AS Min_Date,
        MAX(Date) AS Max_Date
    FROM stock_prices
""").collect()[0]

min_date = date_range["Min_Date"]
max_date = date_range["Max_Date"]

print("Minimum stock date:", min_date)
print("Maximum stock date:", max_date)

# Create one row for every calendar date
dim_date_df = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(min_date)),
                F.to_date(F.lit(max_date)),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("Date")
    )
    .withColumn("Year", F.year("Date"))
    .withColumn("Month", F.month("Date"))
    .withColumn("Month_Name", F.date_format("Date", "MMMM"))
    .withColumn("Month_Short", F.date_format("Date", "MMM"))
    .withColumn("Quarter", F.concat(F.lit("Q"), F.quarter("Date")))
    .withColumn("Day", F.dayofmonth("Date"))
    .withColumn("Day_Name", F.date_format("Date", "EEEE"))
)

# Save the date dimension
dim_date_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_date")

print("\ndim_date table created successfully")

print("\nTotal dates:")
spark.sql("""
    SELECT COUNT(*) AS Total_Dates
    FROM dim_date
""").show()

print("Sample:")
spark.sql("""
    SELECT *
    FROM dim_date
    ORDER BY Date
    LIMIT 10
""").show(truncate=False)


# In[14]:


# ============================================================
# STEP 13 — CREATE STOCK PRICE HISTORY TABLE
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    DoubleType,
    LongType,
    StringType
)

history_schema = StructType([
    StructField("Date", TimestampType(), False),
    StructField("Open", DoubleType(), False),
    StructField("High", DoubleType(), False),
    StructField("Low", DoubleType(), False),
    StructField("Close", DoubleType(), False),
    StructField("Volume", LongType(), False),
    StructField("Symbol", StringType(), False),
    StructField("Previous_Close", DoubleType(), True),
    StructField("Daily_Change_Pct", DoubleType(), True),
    StructField("Snapshot_Timestamp", TimestampType(), False)
])

# Create an empty DataFrame with the required schema
empty_history_df = spark.createDataFrame(
    [],
    history_schema
)

# Create the Delta table
empty_history_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("stock_prices_history")

print("stock_prices_history table created successfully")

print("\nTable structure:")
spark.sql("""
    DESCRIBE stock_prices_history
""").show(truncate=False)

print("\nInitial row count:")
spark.sql("""
    SELECT COUNT(*) AS History_Rows
    FROM stock_prices_history
""").show()


# In[15]:


# ============================================================
# STEP 14 — SAVE FIRST HISTORICAL SNAPSHOT
# ============================================================

from pyspark.sql import functions as F

# Add the current pipeline run timestamp
snapshot_df = stock_prices_spark.withColumn(
    "Snapshot_Timestamp",
    F.current_timestamp()
)

# Select columns in the exact history-table order
snapshot_df = snapshot_df.select(
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Symbol",
    "Previous_Close",
    "Daily_Change_Pct",
    "Snapshot_Timestamp"
)

# Save the first snapshot
snapshot_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("stock_prices_history")

print("First historical snapshot saved successfully")

print("\nHistory validation:")

spark.sql("""
    SELECT
        COUNT(*) AS Total_History_Rows,
        COUNT(DISTINCT Symbol) AS Companies,
        MIN(Snapshot_Timestamp) AS First_Snapshot,
        MAX(Snapshot_Timestamp) AS Latest_Snapshot
    FROM stock_prices_history
""").show(truncate=False)


# In[16]:


# ============================================================
# STEP 15 — CHECK INCREMENTAL LOAD STATUS
# ============================================================

# Compare the newly extracted API data with production data
production_df = spark.table("stock_prices")

api_keys_df = combined_df[
    ["Symbol", "Date"]
].copy()

api_keys_spark = spark.createDataFrame(api_keys_df)

# Existing records in production
existing_count = (
    api_keys_spark
    .join(
        production_df.select("Symbol", "Date"),
        on=["Symbol", "Date"],
        how="inner"
    )
    .count()
)

# New records that don't exist in production
new_count = (
    api_keys_spark
    .join(
        production_df.select("Symbol", "Date"),
        on=["Symbol", "Date"],
        how="left_anti"
    )
    .count()
)

print("INCREMENTAL LOAD CHECK")
print("=" * 60)

print("API records:", len(combined_df))
print("Existing records:", existing_count)
print("New records:", new_count)

if new_count == 0:
    print("\nNo new Symbol + Date records detected.")
else:
    print(f"\n{new_count} new records detected.")


# In[17]:


# ============================================================
# STEP 16 — SAFE UPSERT TEST
# ============================================================

# Remove old test table if it exists
spark.sql("DROP TABLE IF EXISTS stock_prices_upsert_test")

# Create temporary test table
spark.sql("""
    CREATE TABLE stock_prices_upsert_test
    USING DELTA
    AS
    SELECT *
    FROM stock_prices
""")

print("Temporary UPSERT test table created")

initial_count = spark.table("stock_prices_upsert_test").count()

print("Initial rows:", initial_count)

# ------------------------------------------------------------
# TEST 1 — INSERT NEW RECORD
# ------------------------------------------------------------

test_insert = spark.sql("""
    SELECT
        CAST('2026-10-06' AS TIMESTAMP) AS Date,
        100.00 AS Open,
        102.00 AS High,
        99.00 AS Low,
        101.00 AS Close,
        CAST(1000000 AS BIGINT) AS Volume,
        'TEST' AS Symbol,
        98.00 AS Previous_Close,
        3.06 AS Daily_Change_Pct
""")

test_insert.createOrReplaceTempView("test_insert")

spark.sql("""
    INSERT INTO stock_prices_upsert_test
    SELECT *
    FROM test_insert
""")

after_insert = spark.table(
    "stock_prices_upsert_test"
).count()

print("\nINSERT test completed")
print("Rows after INSERT:", after_insert)

# ------------------------------------------------------------
# TEST 2 — UPDATE EXISTING RECORD
# ------------------------------------------------------------

spark.sql("""
    UPDATE stock_prices_upsert_test
    SET Close = 999.99
    WHERE Symbol = 'AAPL'
      AND Date = '2026-05-14'
""")

print("\nUPDATE test completed")

# Verify the update
spark.sql("""
    SELECT
        Symbol,
        Date,
        Close
    FROM stock_prices_upsert_test
    WHERE Symbol = 'AAPL'
      AND Date = '2026-05-14'
""").show()

# ------------------------------------------------------------
# CLEAN UP
# ------------------------------------------------------------

spark.sql("""
    DROP TABLE stock_prices_upsert_test
""")

print("Temporary UPSERT test table removed")
print("Production stock_prices table was not modified.")


# In[18]:


# ============================================================
# STEP 17 — PRODUCTION UPSERT
# ============================================================

# Convert the latest validated API data to a Spark DataFrame
upsert_df = spark.createDataFrame(combined_df)

# Register it as a temporary view
upsert_df.createOrReplaceTempView("incoming_stock_data")

# Perform MERGE / UPSERT
spark.sql("""
    MERGE INTO stock_prices AS target
    USING incoming_stock_data AS source

    ON target.Symbol = source.Symbol
       AND target.Date = source.Date

    WHEN MATCHED THEN UPDATE SET
        target.Open = source.Open,
        target.High = source.High,
        target.Low = source.Low,
        target.Close = source.Close,
        target.Volume = source.Volume,
        target.Previous_Close = source.Previous_Close,
        target.Daily_Change_Pct = source.Daily_Change_Pct

    WHEN NOT MATCHED THEN INSERT (
        Date,
        Open,
        High,
        Low,
        Close,
        Volume,
        Symbol,
        Previous_Close,
        Daily_Change_Pct
    )
    VALUES (
        source.Date,
        source.Open,
        source.High,
        source.Low,
        source.Close,
        source.Volume,
        source.Symbol,
        source.Previous_Close,
        source.Daily_Change_Pct
    )
""")

print("Production UPSERT completed successfully")

# Validate production table
spark.sql("""
    SELECT
        COUNT(*) AS Total_Rows,
        COUNT(DISTINCT Symbol) AS Companies,
        COUNT(DISTINCT CONCAT(Symbol, '_', CAST(Date AS STRING))) AS Unique_Records,
        MIN(Date) AS Earliest_Date,
        MAX(Date) AS Latest_Date
    FROM stock_prices
""").show(truncate=False)


# In[19]:


# ============================================================
# STEP 18 — SAVE CURRENT RUN TO HISTORY
# ============================================================

from pyspark.sql import functions as F

# Prepare current production data for historical storage
history_run_df = spark.table("stock_prices") \
    .withColumn(
        "Snapshot_Timestamp",
        F.current_timestamp()
    ) \
    .select(
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Symbol",
        "Previous_Close",
        "Daily_Change_Pct",
        "Snapshot_Timestamp"
    )

# Append this pipeline run to history
history_run_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("stock_prices_history")

print("Current pipeline run saved to history")

print("\nHistory validation:")

spark.sql("""
    SELECT
        COUNT(*) AS Total_History_Rows,
        COUNT(DISTINCT Symbol) AS Companies,
        COUNT(DISTINCT Snapshot_Timestamp) AS Snapshot_Count,
        MIN(Snapshot_Timestamp) AS First_Snapshot,
        MAX(Snapshot_Timestamp) AS Latest_Snapshot
    FROM stock_prices_history
""").show(truncate=False)


# In[20]:


# ============================================================
# STEP 19 — CREATE COMPANY KPI TABLE
# ============================================================

from pyspark.sql import functions as F

company_kpi_df = spark.sql("""
    SELECT
        Symbol,

        COUNT(*) AS Trading_Days,

        ROUND(MIN(Low), 2) AS Lowest_Price,

        ROUND(MAX(High), 2) AS Highest_Price,

        ROUND(AVG(Close), 2) AS Average_Close,

        ROUND(AVG(Daily_Change_Pct), 2)
            AS Average_Daily_Change_Pct,

        ROUND(
            SUM(
                CASE
                    WHEN Daily_Change_Pct > 0 THEN 1
                    ELSE 0
                END
            ) * 100.0 / COUNT(*),
            2
        ) AS Positive_Days_Pct,

        ROUND(
            SUM(
                CASE
                    WHEN Daily_Change_Pct < 0 THEN 1
                    ELSE 0
                END
            ) * 100.0 / COUNT(*),
            2
        ) AS Negative_Days_Pct,

        SUM(Volume) AS Total_Trading_Volume,

        ROUND(
            AVG(High - Low),
            2
        ) AS Average_Intraday_Range

    FROM stock_prices

    GROUP BY Symbol

    ORDER BY Symbol
""")

# Save KPI table
company_kpi_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("company_kpi")

print("company_kpi table created successfully")

print("\nCompany KPI summary:")
spark.sql("""
    SELECT *
    FROM company_kpi
    ORDER BY Symbol
""").show(truncate=False)


# In[21]:


# ============================================================
# STEP 20 — CREATE DAILY PRICE TREND TABLE
# ============================================================

from pyspark.sql import functions as F

price_trend_df = spark.sql("""
    SELECT
        Date,
        Symbol,
        Close,
        Daily_Change_Pct,
        Volume
    FROM stock_prices
    ORDER BY Date, Symbol
""")

price_trend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("price_trend")

print("price_trend table created successfully")

print("\nValidation:")

spark.sql("""
    SELECT
        COUNT(*) AS Total_Rows,
        COUNT(DISTINCT Symbol) AS Companies,
        MIN(Date) AS Earliest_Date,
        MAX(Date) AS Latest_Date
    FROM price_trend
""").show(truncate=False)


# In[22]:


# ============================================================
# STEP 21 — FINAL DATA VALIDATION
# ============================================================

print("FINAL DATA VALIDATION")
print("=" * 70)

tables = [
    "stock_prices",
    "stock_prices_history",
    "dim_company",
    "dim_date",
    "company_kpi",
    "price_trend"
]

for table_name in tables:
    row_count = spark.table(table_name).count()
    print(f"{table_name:<25} {row_count:>6} rows")

print("=" * 70)

# Check production uniqueness
duplicate_count = spark.sql("""
    SELECT
        COUNT(*) - COUNT(DISTINCT CONCAT(Symbol, '_', CAST(Date AS STRING)))
        AS Duplicate_Count
    FROM stock_prices
""").collect()[0]["Duplicate_Count"]

# Check company count
company_count = spark.sql("""
    SELECT COUNT(DISTINCT Symbol)
    FROM stock_prices
""").collect()[0][0]

print("Production duplicate records:", duplicate_count)
print("Production companies:", company_count)

if duplicate_count == 0 and company_count == 5:
    print("\n✅ FINAL VALIDATION PASSED")
else:
    print("\n❌ FINAL VALIDATION FAILED")


# In[23]:


# ============================================================
# CURRENT LAKEHOUSE CHECK
# ============================================================

tables = [
    "stock_prices",
    "stock_prices_history",
    "dim_company",
    "dim_date",
    "company_kpi",
    "price_trend"
]

print("CURRENT LAKEHOUSE STATUS")
print("=" * 60)

for table_name in tables:
    count = spark.table(table_name).count()
    print(f"{table_name:<25} {count:>6} rows")

print("=" * 60)

duplicate_count = spark.sql("""
    SELECT
        COUNT(*) -
        COUNT(DISTINCT CONCAT(Symbol, '_', CAST(Date AS STRING)))
        AS Duplicate_Count
    FROM stock_prices
""").collect()[0]["Duplicate_Count"]

company_count = spark.sql("""
    SELECT COUNT(DISTINCT Symbol)
    FROM stock_prices
""").collect()[0][0]

print("Production duplicates:", duplicate_count)
print("Companies:", company_count)

