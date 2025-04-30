# 💳 Financial Transaction Monitoring System – PySpark Edition

A scalable financial fraud detection system built with Apache Spark. This project simulates 50,000+ transaction records and applies real-world detection logic including geolocation anomalies, transaction velocity, and weighted fraud scoring. Built using PySpark to handle scale and distributed compute.

---

## 🚀 Project Overview

This project simulates high-volume bank transactions and applies advanced fraud detection logic. It uses PySpark’s distributed engine to process the data at scale and builds a weighted risk scoring system for downstream alerting and investigation pipelines.

---

## 🛠️ Tech Stack

- **PySpark**: Distributed ETL, anomaly detection
- **Spark SQL + Window Functions**: Lag-based velocity checks
- **Matplotlib / Seaborn**: Visualization of fraud score distribution
- **Jupyter Notebook**: Simulation interface
- **CSV Output**: Simulates Snowflake/Redshift ingestion

---

## 📈 Key Features

- ✅ Simulates 50,000+ synthetic financial transactions
- ✅ Applies rule-based fraud logic:
  - Cross-border check
  - Location switching anomaly within time window
  - High-amount flags
- ✅ Assigns dynamic fraud scores (0–100 scale)
- ✅ Flags high-risk transactions for alerting
- ✅ Outputs cleaned + flagged data for cloud analytics or reporting tools

---

## 🔎 Fraud Detection Logic

| Rule | Score |
|------|-------|
| Amount > ₹5000 | +30 |
| Foreign transaction (non-Delhi/Mumbai) | +30 |
| Geo switch within 10 minutes | +40 |

> Final fraud score capped at 100

---

## 📂 Project Structure

```
financial-transaction-monitoring-pyspark/
├── notebooks/
│   └── financial_transaction_monitoring_pyspark.ipynb
├── high_risk_transactions_output/
├── .gitignore
├── LICENSE
├── requirements.txt
└── README.md

```

---

## 🚦 Output Snapshot

> Sample output: `high_risk_transactions_output/`

- `transaction_id`
- `account_id`
- `location`
- `amount`
- `fraud_score`
- `geo_anomaly`

---

## 📦 Scalability Notes

This notebook runs locally with a SparkSession for demonstration. In production, this design can scale to:

- **Apache Spark on EMR or Databricks**
- **Real-time ingestion from Kafka or Kinesis**
- **ETL on AWS Glue**
- **Alerting via Lambda + SNS**
- **Data warehousing on Redshift or Snowflake**

---

## ✅ Getting Started

1. Clone the repo:
   ```bash
   git clone https://github.com/yourusername/financial-transaction-monitoring-pyspark.git
   cd financial-transaction-monitoring-pyspark
   ```

2. Create conda env:
   ```bash
   conda create -n spark-env python=3.10
   conda activate spark-env
   pip install -r requirements.txt
   ```

3. Run notebook:
   ```bash
   jupyter notebook notebooks/financial_transaction_monitoring_pyspark.ipynb
   ```

---

## 📬 Contact

- **Author:** Paramveer Singh Shekhawat  
- **Email:** rahulshekhawat408@gmail.com  
- **LinkedIn:** [LinkedIn Profile](https://www.linkedin.com/in/paramveer-singh-shekhawat-376a1a244/)  
- **GitHub:** [GitHub Profile](https://github.com/Param-2003)

---

## 📄 License

This project is licensed under the MIT License.  
You are free to use, modify, and distribute this code with attribution.

---
