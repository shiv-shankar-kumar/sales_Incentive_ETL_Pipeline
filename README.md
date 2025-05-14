
🛒 Sales Performance and Customer Engagement Pipeline

This project showcases a real-world data engineering solution developed for a major offline grocery and kitchen supplies retailer. The goal was to build a robust, scalable data pipeline for sales analytics and customer retention using modern big data tools and cloud services.

🚀 Project Objectives
- Analyze Point-of-Sale (POS) data to monitor store-level sales performance.
- Build an incentive system for high-performing sales staff.
- Identify infrequent buyers and boost retention through targeted incentives.
- Optimize data processing performance on large datasets (~100 GB/day).

- 🛠️ Tech Stack
- Data Engineering: Apache Spark, Python, SQL
- Orchestration: Apache Airflow, CRON jobs
- Cloud Services: Azure Data Lake Storage (ADLS), Azure DevOps CI/CD
- Data Modeling: Dimensional Modeling (Fact and Dimension Tables)

🧩 Key Features
- ETL Pipelines: Built scalable Spark-based pipelines for ingesting and transforming POS data.
- Incentive Engine: Developed logic to rank salespeople by performance and generate incentive reports.
- Customer Retention Module: Identified infrequent buyers and generated personalized coupon offers.
- Performance Optimization: Used Spark techniques such as:
        Caching, Broadcast joins, Predicate pushdown, Projection pruning
- Workflow Automation: Scheduled daily jobs using Airflow and integrated CI/CD pipelines via Azure DevOps.

 Outcomes
- Improved pipeline execution times through optimization strategies.
- Increased salesperson motivation through performance-based incentives.
- Boosted customer retention with data-driven coupon campaigns.

sample data : 
Fact table(transactional data ) : 


Dimension table (with star schema) : 
mysql> describe customer;
### 🧾 `customer` Table Schema

| Field                 | Type         | Null | Key | Default | Extra          |
|----------------------|--------------|------|-----|---------|----------------|
| customer_id          | int          | NO   | PRI | NULL    | auto_increment |
| first_name           | varchar(50)  | YES  |     | NULL    |                |
| last_name            | varchar(50)  | YES  |     | NULL    |                |
| address              | varchar(255) | YES  |     | NULL    |                |
| pincode              | varchar(10)  | YES  |     | NULL    |                |
| phone_number         | varchar(20)  | YES  |     | NULL    |                |
| customer_joining_date| date         | YES  |     | NULL    |                |





mysql> describe product;

| Field          | Type          | Null | Key | Default | Extra           |
| -------------- | ------------- | ---- | --- | ------- | --------------- |
| id             | int           | NO   | PRI | NULL    | auto\_increment |
| name           | varchar(255)  | YES  |     | NULL    |                 |
| current\_price | decimal(10,2) | YES  |     | NULL    |                 |
| old\_price     | decimal(10,2) | YES  |     | NULL    |                 |
| created\_date  | timestamp     | YES  |     | NULL    |                 |
| updated\_date  | timestamp     | YES  |     | NULL    |                 |
| expiry\_date   | date          | YES  |     | NULL    |                 |


mysql> describe sales_team;

| Field         | Type         | Null | Key | Default | Extra           |
| ------------- | ------------ | ---- | --- | ------- | --------------- |
| id            | int          | NO   | PRI | NULL    | auto\_increment |
| first\_name   | varchar(50)  | YES  |     | NULL    |                 |
| last\_name    | varchar(50)  | YES  |     | NULL    |                 |
| manager\_id   | int          | YES  |     | NULL    |                 |
| is\_manager   | char(1)      | YES  |     | NULL    |                 |
| address       | varchar(255) | YES  |     | NULL    |                 |
| pincode       | varchar(10)  | YES  |     | NULL    |                 |
| joining\_date | date         | YES  |     | NULL    |                 |



mysql> describe store;

| Field                | Type         | Null | Key | Default | Extra |
| -------------------- | ------------ | ---- | --- | ------- | ----- |
| id                   | int          | NO   | PRI | NULL    |       |
| address              | varchar(255) | YES  |     | NULL    |       |
| store\_pincode       | varchar(10)  | YES  |     | NULL    |       |
| store\_manager\_name | varchar(100) | YES  |     | NULL    |       |
| store\_opening\_date | date         | YES  |     | NULL    |       |
| reviews              | text         | YES  |     | NULL    |       |



and the final outcome : 

mysql> describe customer_data_mart_table;

| Field              | Type   | Null | Key | Default | Extra |
| ------------------ | ------ | ---- | --- | ------- | ----- |
| customer\_id       | int    | YES  |     | NULL    |       |
| full\_name         | text   | YES  |     | NULL    |       |
| store\_address     | text   | YES  |     | NULL    |       |
| phone\_number      | text   | YES  |     | NULL    |       |
| sales\_date\_month | text   | YES  |     | NULL    |       |
| total\_sales       | double | YES  |     | NULL    |       |


and final table in which we can check the incentive : 
mysql> describe sales_team_data_mart;

| Field             | Type          | Null | Key | Default | Extra |
| ----------------- | ------------- | ---- | --- | ------- | ----- |
| store\_id         | int           | YES  |     | NULL    |       |
| sales\_person\_id | int           | YES  |     | NULL    |       |
| full\_name        | varchar(255)  | YES  |     | NULL    |       |
| sales\_month      | varchar(10)   | YES  |     | NULL    |       |
| total\_sales      | decimal(10,2) | YES  |     | NULL    |       |
| incentive         | decimal(10,2) | YES  |     | NULL    |       |
