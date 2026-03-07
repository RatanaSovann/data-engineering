# 🚗 NYC Data Engineering End-to-End Project

## Objective

1. To ingest and prepare yellow and green taxi trip records from the New York City Taxi and Limousine Commission (TLC), analysing over 1 billion rows of data from 2014–2024 using Apache Spark on Databricks.
2. Clean and transform the dataset to ensure it is suitable for large-scale analysis and modelling.
3. Use SparkSQL to perform aggregations and answer key business and operational questions about taxi activity.
4. Develop and evaluate two machine learning models to predict total trip fare using Scikit-learn, with performance measured by RMSE.
5. To demonstrate the use of distributed data processing and machine learning to generate actionable insights from massive real-world transport datasets.

## Dataset Used

The New York City Taxi and Limousine Commission (TLC) has been responsible for licensing and regulating the city’s taxi services since 1971. The agency has publicly released millions of trip records from both yellow and green taxis, with the dataset containing 908 million yellow taxi records and 83 million green taxi records. Each record includes details such as pick-up and drop-off dates and times, locations, trip distances, itemized fares, rate types, payment methods, and driver-reported passenger counts.
In addition, a taxi_lookup.csv file was also provided to match the pick-up and drop-off locations code to their zone and borough names.

## Technologies

- Language: Python, SQL
- Big Data Processing: Apache Spark, SparkSQL
- Platform: Databricks
- Data Analysis & Development: Jupyter Notebook
- Machine Learning: Scikit-learn
- Evaluation Metric: RMSE

## Part 1: Data Ingestion and Preperation

### Step 1: Download & Load Data:

The dataset for both yellow and green taxis were downloaded using a provided notebook and added to DBFS (Databricks File System). The data was then loaded in a separate notebook in Databrick workspace.
Verifying row counts of the loaded data:

<img width="490" height="211" alt="image" src="https://github.com/user-attachments/assets/5ba8334d-fd87-4c9a-99a6-4aefa98b5e62" />

### Step 2: Explore, Clean & Merge Dataset:
After exploring the shape and schema of both Green and Yellow dataset the following cleaning and transformation was done:
1. Removing invalid date range: remove row with dates outside of what was expected (2014 – 2024)
2. Keep only trips with positive and realistic speed: remove trips with negative speed and exceeding 200mph.
3. Remove trips with unrealistic duration and distance: being conservative, keep only trips between 1min to 5 hours and 0.1 to 100 miles.
4. Remove trips with unrealistic fares: keep fare amount between $3 and $1000.
5. Remove unrealistic passenger count: keep passenger count between 1 and 6.
6. Merge Yellow and Green taxi data: ensure the schemas and columns order are align filling missing column with null values. Then merge using Union based on the columns name.
7. Match pick-up and drop-up zone IDs to their names and boroughs: To ensure safe merge, two different data frames were created:

<img width="640" height="292" alt="image" src="https://github.com/user-attachments/assets/e74ad022-10a5-40a3-b926-264bdd14fdc5" />

Then left join was used on both data frame to match ‘Location_ID’ to its respective zone and borough:

<img width="640" height="73" alt="image" src="https://github.com/user-attachments/assets/0842c806-4e05-463d-9447-f88f2da31735" />

8. Additional Cleaning based on logic: Keep only trips with tips between 0 and $50. Extra charges have to be positive. The ‘mta_tax’ has to be between 0 and 0.5%. The improvement charges have to be between 0 and 1.
9. Final row counts: 955,059,780
10. Save Data: The final combined data frame is saved as a Delta table to perform analysis in Part 2.

Full notebook: 

