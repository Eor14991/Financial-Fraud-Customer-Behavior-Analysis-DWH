Got it 👍 You want this project documentation rewritten in a **README.md (Markdown format)** so it looks clean and professional for GitHub.
Here’s the fixed version:

---

# 💳 Financial Fraud & Customer Behavior Analysis DWH

This repository contains the complete **end-to-end data warehousing project** for analyzing financial transactions.

The project follows a **Medallion Architecture** (Bronze → Silver → Gold) to:

* Ingest raw data
* Clean and transform it
* Model it into a **high-performance star schema** for business intelligence and machine learning.

---

## 📂 Project Structure

```
.
├── 1_bronze_layer/       # Raw data ingestion scripts
├── 2_silver_layer/       # Data cleaning, transformation & integration
├── 3_gold_layer/         # Star schema scripts for analytics
├── 4_powerbi_dashboard/  # Final Power BI report file
└── other_scripts/        # Miscellaneous testing & exploration queries
```

---

## 🚀 How to Run This Project

Follow these steps in sequence to build the data warehouse and reproduce the analysis.

---

### 1️⃣ Bronze Layer: Raw Data Ingestion

1. **Create Bronze Schema & Tables**

   ```sql
   Run 1_create_bronze_schema.sql
   ```

   * Creates the **bronze schema** and **staging tables**.
   * Uses flexible `NVARCHAR(MAX)` to ensure raw load never fails.

2. **Load Raw Data**

   ```sql
   Update & Run 2_load_bronze_layer.sql
   ```

   * Replace placeholder file paths with the **actual CSV/JSON files**.
   * Truncates and bulk inserts data into the staging tables.

---

### 2️⃣ Silver Layer: Clean & Conformed Data

1. **Create Silver Schema & Tables**

   ```sql
   Run 1_create_silver_schema.sql
   ```

   * Defines **structured tables** with correct data types (`INT`, `DECIMAL`, `DATE`, etc.).

2. **Load Clean Data (Choose ONE method):**

   * **Option A (SQL Only):**

     ```sql
     Run 2a_load_silver_layer.sql
     ```

     Cleans data (removes symbols, converts dates, etc.) and loads it into Silver tables.

   * **Option B (Python + Pandas):**

     ```python
     Run 2b_bronze_to_silver_etl.ipynb
     ```

     Reads Bronze data → transforms with **pandas** → writes back to Silver tables.

---

### 3️⃣ Gold Layer: Star Schema

1. **Create Star Schema Tables**

   ```sql
   Run 1_create_gold_star_schema.sql
   ```

   * Builds **Fact & Dimension tables**.

2. **Load Data into Star Schema**

   ```sql
   Run 2_load_gold_layer.sql
   ```

   * Populates Fact/Dim tables from Silver → final **Gold star schema**.

---

### 4️⃣ Visualization

* Open the Power BI report:

  ```
  4_powerbi_dashboard/finance.pbix
  ```
* Update **data source credentials** to connect to your SQL Server instance.
* Explore the **Financial Fraud & Customer Behavior dashboard**.

---

✅ With this setup, you have a **full DWH pipeline**:

* **Bronze:** Raw Data
* **Silver:** Clean & Integrated Data
* **Gold:** Star Schema for BI & ML
* **Power BI:** Interactive Visualizations
