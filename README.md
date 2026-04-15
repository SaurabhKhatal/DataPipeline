# 🚀 Data Pipeline for Multi-Vendor Sales Aggregation

## 📌 Overview

This project builds a robust data pipeline to ingest, clean, standardize, and aggregate sales data from multiple B2B vendors:

* Blinkit
* Zepto
* Nykaa
* Myntra

The pipeline handles schema inconsistencies, missing values, and different revenue logic across vendors.

---

## ⚙️ Pipeline Flow

Ingestion → Cleaning → Standardization → Aggregation → Output

---

## 🧾 Data Challenges Solved

* Different column names across vendors
* Multiple date formats
* Missing and inconsistent values
* Different revenue definitions per platform

---

## 🔄 Revenue Logic per Source

| Source  | Revenue Logic               |
| ------- | --------------------------- |
| Blinkit | `mrp`                       |
| Zepto   | `Gross Merchandise Value`   |
| Nykaa   | `Selling Price`             |
| Myntra  | `mrp_revenue - vendor_disc` |

---

## 📤 Output Schema

| Column        | Description        |
| ------------- | ------------------ |
| date          | Transaction date   |
| sku           | Product identifier |
| total_units   | Total units sold   |
| total_revenue | Total revenue      |
| data_source   | Source of data     |

---

## 🛠 Tech Stack

* Python
* Pandas

---

## 🚀 How to Run

```bash
python pipeline.py
```

---

## 📁 Project Structure

```
DataPipeline/
│
├── pipeline.py
├── README.md
├── requirements.txt
├── .gitignore
```

---

## 🔮 Future Improvements

* Airflow scheduling
* Database integration (PostgreSQL/Snowflake)
* Logging & monitoring
* Real-time ingestion

---

## 👨‍💻 Author

Saurabh Khatal
