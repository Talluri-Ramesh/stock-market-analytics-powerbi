# 📈 Stock Market Analytics Dashboard

An end-to-end Stock Market Analytics project built using **Microsoft Fabric, Python, and Power BI** to collect, process, analyze, and visualize daily stock market data for five companies.

# 📌 Project Overview

This project focuses on analyzing historical stock market performance through automated data pipelines, data cleaning, transformation, data modelling, and interactive Power BI dashboards.

The project integrates Python-based data processing with Microsoft Fabric Lakehouse and Power BI Direct Lake to provide meaningful insights into stock price movements, trading volume, company performance, and market trends.

# 🎯 Project Objectives

- Automate daily stock market data collection.
- Clean and validate historical stock market data.
- Build a structured analytical data model.
- Analyze stock price and trading volume trends.
- Develop interactive dashboards for business-friendly reporting.

## 🛠️ Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data extraction and processing |
| Microsoft Fabric | Data engineering and Lakehouse |
| Power BI | Interactive dashboards |
| Alpha Vantage API | Stock market data source |
| PySpark | Data transformation |
| SQL | Data analysis and modelling |
| GitHub | Project version control |

## 🏢 Companies Analyzed

- IBM
- Apple (AAPL)
- JPMorgan Chase (JPM)
- Amazon (AMZN)
- Coca-Cola (KO)

## 📊 Power BI Dashboard

The dashboard contains five pages:

### 1. Home Page
Project landing page and navigation.

### 2. Executive Overview
High-level overview of stock market performance.

### 3. Company Performance
Comparison of stock performance across companies.

### 4. Price & Volume Analysis
Analysis of stock price movements and trading volume.

### 5. Market Trends
Historical market trends and stock performance patterns.

## 📷 Dashboard Previews

### Home Page
![Home Page](Screenshots/Home.png)

### Executive Overview
![Executive Overview](Screenshots/Executive%20Overview%20Page.png)

### Company Performance
![Company Performance](Screenshots/Company%20Performance.png)

### Price & Volume Analysis
![Price and Volume Analysis](Screenshots/Price%20%26%20Volume%20Analysis.png)

### Market Trends
![Market Trends](Screenshots/Market%20Trends.png)

## 🗂️ Data Model

![Stock Market Analytics Data Model](Screenshots/Stock%20Market%20Analytics%20Model.png)

## ⚙️ Data Pipeline

1. Extract daily stock market data using the Alpha Vantage API.
2. Load the extracted data into Microsoft Fabric Lakehouse.
3. Clean and validate records using Python and Spark.
4. Handle duplicate records and missing values.
5. Calculate daily percentage changes and intraday price ranges.
6. Organize the data into analytical tables.
7. Connect Power BI to the semantic model using Direct Lake.

## 📂 Project Files

- `Ramesh_Stock_Market_Analytics.pbix` – Power BI dashboard.
- `Ramesh_StockMarket_DataPipeline.py` – Stock market data pipeline.
- `Ramesh-Stock_Market_Daily_Refresh.ipynb` – Daily refresh notebook.
- `Screenshots/` – Dashboard previews and data model.

## 🚀 Key Features

- Automated daily stock data extraction.
- Data cleaning and validation.
- Historical stock market analysis.
- Company-wise performance comparison.
- Interactive Power BI dashboards.
- Microsoft Fabric Lakehouse integration.
- Direct Lake semantic model.

## 📚 Learning Outcomes

- Hands-on experience with Microsoft Fabric.
- Building data pipelines using Python.
- Working with external financial APIs.
- Data transformation and validation.
- Data modelling and Power BI dashboard development.
- Turning raw financial data into meaningful insights.

## 👨‍💻 Author

**Talluri Ramesh Babu**

B.Tech – Computer Science and Engineering

- GitHub: [Talluri-Ramesh](https://github.com/Talluri-Ramesh)
- LinkedIn: [Talluri Ramesh Babu](https://www.linkedin.com/in/talluri-ramesh-babu-32ba6a269/)

---

⭐ If you find this project useful, consider giving this repository a star!
