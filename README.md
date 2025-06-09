# 🛒 Sales Performance and Customer Engagement Pipeline  
An ETL solution for sales analytics and customer retention in the retail grocery industry.

---

## 📌 Project Overview

### 🧠 Background  
In the retail grocery and kitchen supplies industry, understanding sales performance, customer behavior, and optimizing staff incentives is critical for profitability. Raw sales data often exists in multiple formats and sources, making it difficult to extract actionable insights. This project focuses on implementing an ETL (Extract, Transform, Load) pipeline to clean, aggregate, and model this data, making it suitable for business intelligence and decision-making.

The goal is to automate the extraction, transformation, and loading of data for better sales analysis and customer engagement.

---

### 🎯 Purpose  
The objective of this project is to support data-driven sales decisions and customer retention strategies by building an end-to-end ETL pipeline. The key tasks include:

- Extracting sales data from various sources  
- Cleaning and transforming the data for analysis  
- Building incentives and performance reports for sales staff  
- Identifying infrequent buyers and increasing retention with targeted offers  
- Uploading cleaned and transformed data to Azure Data Lake for finance team analysis  

---

## 🔍 Overview of the Code

The project is divided into four main parts, focusing on each step of the ETL process. The entire pipeline is executed through the final function, which orchestrates data extraction, transformation, and loading into Azure Data Lake.


---

## ⚙️ ETL Pipeline Details

### **Ingestion:**  
🔗 [ingestion_db.ipynb](https://github.com/shiv-shankar-kumar/Sales-Performance-Analytics/blob/main/ingestion_db.ipynb)  
- Raw sales and customer data loaded from CSV files using Python and Pandas.

### **Transformation:**  
🔗 [Transformation & Load Notebook](https://github.com/shiv-shankar-kumar/Sales-Performance-Analytics/blob/main/Transformation%26Load.ipynb)  
- Data is cleaned and standardized using Pandas, and SQL queries are used for aggregation.  
- Incentive and retention logic is applied using Python and SQL.

### **Load:**  
- Transformed data is loaded into Azure Data Lake, where it is made available for further analysis by the finance and operations teams.

---

## 📌 Final Output Tables

### 1. **`sales_team_data_mart`**  
Includes:
- Salesperson performance data
- Monthly sales totals and incentive calculations
- Store-level sales performance insights

### 2. **`final_customer_data_mart`**  
Includes:
- Customer purchase performance data  
- Monthly billing data by customers  
- Customer-level billing performance insights 

---

## 📊 Business Insights  

🔗 [Business Queries Notebook](https://github.com/shiv-shankar-kumar/Sales-Performance-Analytics/blob/main/VP_Analysis.ipynb)

| Business Question | Insight Method |
|-------------------|----------------|
| Which salespeople are the top performers? | Grouped by salesperson with total sales |
| How can we boost retention for infrequent buyers? | Targeted promotions and discounts |
| What are the sales trends at the store level? | Aggregated sales per store and per month |
| How can we optimize the pipeline for large data? | Spark-based optimizations like caching and broadcast joins |

---

## 💰 Business Impact Summary  

- **Total Sales:** $320M  
- **Total Incentives Paid:** $5.6M  
- **Sales Team Performance Increase:** 15%  
- **Customer Retention Boost:** 12% increase in repeat customers

---

## 🧮 Resources

### **Data Sources**  
- Raw sales, customer, and POS data in CSV format

### **Environment**  
- Python 3.x  
- Apache Spark  
- SQL  
- Azure Data Lake Storage

### **Dependencies**  
- pandas  
- pyspark  
- sqlalchemy  
- airflow  
- azure-storage-blob  

### **Software**  
- Azure DevOps for CI/CD  
- Apache Airflow for orchestration  
