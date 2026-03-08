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

Full notebook: [Data_Ingestion.ipynb](https://github.com/RatanaSovann/data-engineering/blob/main/Data%20Processing_%20NYC_Taxi/Data_Ingestion.ipynb)

## Part 2: Ad-Hoc Analysis & Business Questions

Full notebook: [Ad-Hoc_Analysis.ipynb](https://github.com/RatanaSovann/data-engineering/blob/main/Data%20Processing_%20NYC_Taxi/Ad-Hoc_Analysis.ipynb)

<img width="1177" height="514" alt="image" src="https://github.com/user-attachments/assets/97c98f86-8b17-4f70-8751-99e8abc07c98" />

### Q1. For each year and month:
#### - What was the total number of trips?:

The total number of trips fluctuated over the years. The month-on-month trip ranges from 9 million trips – 16 million trips (pre Covid-19). During the pandemic total trips drop significantly with the lowest trip number in April 2020, totaling 223,961 trips. Since then monthly trip number hovers around 2-3 million trips.

#### a. Which day of week had the most trips?:

Fridays consistently had the most trips across the years.

#### b. Which hour of the day had the most trips?:

The hour with the most trips was consistently in the evening, between 6pm – 9pm, potentially due to post work entertainment or dinner plans.

#### c. What was the average number of passengers?:

The averages passenger count is between 1 and 2 people.

#### d. What was the average amount paid per trip (using total_amount)?:

The average fare per trip increased over time, starting from around $14.43 in early 2014 and rising to around $29.43 by December 2024.

#### e. What was the average amount paid per passenger (using total_amount)?:

The average fare per passenger followed the same trend, rising from about $11 in 2014 to $25 by the end of 2024.

<img width="1541" height="112" alt="image" src="https://github.com/user-attachments/assets/91466de1-8478-42e8-8533-7910d06c48f7" />

### Q2. For each taxi colour (yellow and green):
#### a. What was the average, median, minimum and maximum trip duration in minutes?:

Green taxis averages 13.63 minutes per trip, while yellow taxis average slightly longer at 14.42 minutes. Median durations are 10.55 minutes for green and 11.28 minutes for yellow taxis, with both having a minimum of 1 minutes and a maximum of 300 minutes.

#### b. What was the average, median, minimum and maximum trip distance in km?:

The average trip distance is similar, 4.79 km for green and 4.91 km for yellow taxis. Green taxis have a higher median distance (3.12 km vs. 2.75 km). Both share a minimum of 0.16 km and a maximum of 160 km. From Q.2a Green taxis likely serve longer trips, operating outside central Manhattan.

#### c. What was the average, median, minimum and maximum speed in km per hour?:
Green taxis averae 20.44 km/h, slightly faster than yellow taxis at 18.9 km/h. Median speeds tell the same story (18.56 km/h for green & 16.58 km/h for yellow). Both peak at 321 km/h (likely outliers) and drop to 0.04 km/h due to traffic. Overall, Green taxis move faster, operating outside Manhattan's heavy congestion.


<img width="1768" height="577" alt="image" src="https://github.com/user-attachments/assets/84734e1e-3228-450e-869d-6b8605d810d9" />


### Q3. For each taxi colour (yellow and green), each pair of pick up and drop off locations (boroughs), each month, each day of week and each hours:
#### a. What was the total number of trips?:

For Green taxi, out of all the pick-up and drop-off locations buckets of trip, the most amount of trip is 7779 trips (Brooklyn to Brooklyn) on 2015 January, Saturday around 10pm. For Yellow taxi, out of all the pick-up and drop-off locations buckets of trip, the most common is between Manhattan and Mahattan, with the most recorded trip 132,877 trips on 2014 April, Wednesday at 8pm.

#### b. What was the average distance?:

For green taxis on 2015 Jan, Saturday at 7am the highest recorded average distance is 79.52km (Unknown to Queens). For Yellow taxi the highest average distance is 158.5km (Brooklyn to Unknown) on 2013 November, Wednesday at 3pm.

#### c. What was the average amount paid per trip (using total_amount)?:

For the average amount paid per trip, the highest are usually trips that cover long distances. Those trips are not as frequent and are more costly. For Yellow Taxi (Brooklyn to Unknown) had the highest average amount paid per trip which occurred at December 2023, Thursday around 3pm. For green taxis (Brooklyn to Staten Island) had the highest average amount paid per trip which occurred at January 2015, Tuesday around 2pm.

#### d. What was the total amount paid (using total_amount)?:

For Yellow taxi (Manhattan to Manhattan) on 2014 April, Wednesday at 8pm had the highest total amount paid $ 1,566,516 For Green Taxi (Brooklyn to Brooklyn ) on 2015 September, Sunday at 12am had the highest total amount paid $ 8,731,593.

<img width="798" height="388" alt="image" src="https://github.com/user-attachments/assets/f3fe35bc-0109-4694-9760-5c5b819c3af4" />

### Q4. For 2024, compute the share of total revenue contributed by the Top 10 pickup→dropoff borough pairs (ranked by total_amount):

The top 10 revenue generating pick-up and drop-off borough is shown below. Manhattan is by far the busiest with over 61% share of the entire top 10.

<img width="219" height="111" alt="image" src="https://github.com/user-attachments/assets/e9ee2fec-77ac-48c9-92d8-3f2417a888cc" />

### Q5.What was the percentage of trips where drivers received tips?:

The percentage of trips where drivers received tips is 63.26%.

<img width="255" height="112" alt="image" src="https://github.com/user-attachments/assets/f2f2db5f-209f-43f6-b3a2-23c3895ff300" />

### Q6. For trips where the driver received tips, what was the percentage where the driver received tips of at least $15?:

For trips where the driver received tips, the percentage of trips where the driver received tips of at least $15 is 0.81%.

<img width="618" height="237" alt="image" src="https://github.com/user-attachments/assets/fcddd9dd-01ec-409e-be73-bd6fb4c34b89" />

### Q7. Classify each trip into bins of durations. Then for each bin, calculate: Average speed (km per hour), Average distance per dollar (km per $):

a. Under 5 Mins: The average speed is 19.81 km/h with an average distance per dollar of 0.16 km/$.

b. From 5 mins to 10 mins: The average speed is 17.22 km/h with an average distance per dollar of 0.21 km/$.

c. From 10 mins to 20 mins: The average speed is 17.91 km/h with an average distance per dollar of 0.26 km/$.

d. From 20 mins to 30 mins: The average speed is 21.36 km/h with an average distance per dollar of 0.3 km/$.

e. From 30 mins to 60 mins: The average speed is 25.81 km/h with an average distance per dollar of 0.34 km/$.

f. At least 60 mins: The average speed is 22.64 km/h with an average distance per dollar of 0.39 km/$

<img width="955" height="240" alt="image" src="https://github.com/user-attachments/assets/9a9be89d-9e7a-40eb-8e76-dd7cb79218fd" />

### Q8. Which duration bin will you advise a taxi driver to target to maximise his income?:
The highest estimated income per hour is the under 5 mins trips at $122.75/hour.

- These are short, high-frequency trips.
- Even though the fare per trip is low (7.32 AUD), drivers can complete many trips per hour.

It seems like the best strategy is to focus on short trips under 10 minutes to maximise income per hour, whenever those trips are available.

## Part 3: Machine Learning Modelling

This section details the process and approach of building two machine learning models for predicting the total fare of a trip. The metric used to evaluate the models is Root Mean Squared Error (RMSE).

Due to the resource constraints of Data Brick free edition, the full dataset is not used. To reduce computations, all the preprocessing required for modelling is done using Spark Data Frame operations (select, withColumn, write). These run distributed across the serverless cluster’s executors, efficiently handling the 900M rows without overloading the driver.

#### Data Splitting:

Data are split based on date and further take 1% sample and stratified by year–month to preserve the natural distribution of rides across months. A random fraction within each month is used to separate training and validation sets, ensuring that both sets contain data from all months while maintaining representativeness. The split is as follows:
- Testing Set: From 2024-10-01 onward (held out for final evaluation).
- Validation Set: A stratified random 30% sample from months before 2024-10-01.
- Training Set: The remaining stratified random 70% of records from months before 2024-10-01.

#### Baseline Model:

The average cost of a bucket of trips aggregated by a given taxi type that shares the same pickup borough, dropoff borough, month, day of the week and hour of the day is used as a proxy for the total amount. This figure is used as the baseline model to calculate the baseline RMSE.

<img width="1189" height="256" alt="image" src="https://github.com/user-attachments/assets/950735a6-a553-4545-8df7-46c0f599a302" />

Full notebook: [ML_modelling.ipynb](https://github.com/RatanaSovann/data-engineering/blob/main/Data%20Processing_%20NYC_Taxi/ML_Modelling.ipynb)

#### Linear Regression:

Initially, each feature is iteratively added to the model and validated on only one month of data to assess model improvement. The final features used are shown below:

Features selection:

- Numerical Features: trip_distance, trip_duration, year, month, and hour. Among these, trip distance and duration are the primary factors influencing the total fare. Meanwhile, year, month, and hour are crucial for capturing patterns in passenger demand.
- Categorical Features: day_of_week and hour_of_day. These features are One Hot Encoded using Spark.
  
A function is defined to help encode the categorical variables:

<img width="940" height="499" alt="image" src="https://github.com/user-attachments/assets/7210932b-a154-4013-a2ed-b6a758f5baa9" />

#### Elastic Net:

Elastic Net is useful when predictors are highly correlated or when you want a mix of feature selection and coefficient shrinkage.

Model parameter tuning:

<img width="706" height="195" alt="image" src="https://github.com/user-attachments/assets/0e89fee8-181c-418f-b273-81bcac495ebd" />

Noted: Despite increasing regularization overfitting did not improve much, suggesting some issues with the data split or quality of features.

#### XGBoost

XGBoost is a gradient boosting algorithm that builds an ensemble of decision trees sequentially. The parameters below are tuned with the focus on trying to reduce overfitting.

<img width="798" height="676" alt="image" src="https://github.com/user-attachments/assets/b93e8594-79ed-4999-828f-319561567704" />

#### Modelling Results

<img width="539" height="210" alt="image" src="https://github.com/user-attachments/assets/26fb62b0-70ae-4267-b7f7-c9048863affe" />

#### Key Takeaways:

- XGBoost achieved the best overall model performance with the lowest training, validation, and test RMSE.
- It showed stronger generalisation than Linear Regression and Elastic Net, with a smaller train–test error gap.
- While training took longer on 1% of the data, the improvement in accuracy and robustness justified the extra time.
- XGBoost final test RMSE of 7.59 was 37% better than the baseline, meeting the project target.






