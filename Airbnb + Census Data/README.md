#  Airbnb Building ELT data pipelines with Airflow Project

## Objective

This project involve designing and building production-ready data pipelines with Airflow and DBT:
1. Load listing and census data sequentially to the Airflow storage bucket.
2. Using DBeaver, a Bronze schema is set up in the the Postgres instance
3. An Airflow DAG is build to reads data from the storage bucket and loads it into the raw tables within the bronze schema on Postgres.
4. Design a data warehouse architecture in PostgreSQL following the Medallion Architecture framework, which includes Bronze, Silver, and Gold layers.
5. Perform Ad-Hoc Analysis on the full data with PostgreSQL

## Dataset Used
This project combines three datasets to analyse Airbnb activity in Sydney from May 2020 to April 2021 in the context of neighbourhood-level demographics. 
The Airbnb data captures rental patterns, pricing, and review activity, while the ABS Census data provides population and housing insights that support broader social and planning analysis. 
The LGA code mapping files link suburbs to local government areas, enabling Airbnb listings to be matched with relevant census statistics.

The dataset can be found in the following links:
- 12 months of Airbnb listing data for Sydney: https://drive.google.com/file/d/1_AvGzOLrCNCnDJyStSj2XH0bTUtsKgb_/view?usp=sharing
- The tables G01 (“Selected Person Characteristics by Sex”) and G02 (“Selected Medians and Averages”) of the General Community Profile Pack from the 2016 census at the LGA level: https://drive.google.com/file/d/1AbfLWOCgPfAY8bBRX1blZdL0-dO2joXT/view?usp=sharing
- A dataset to help you join both datasets based on LGAs code and a mapping between LGAs and Suburbs: https://drive.google.com/file/d/1y962EkNhG2nBGiMsV8sYN2BeFsIy6zO5/view?usp=sharing/view?usp=sharing

## Technologies

The following technologies are used to build this project:
- Language: Python, SQL
- Extraction and transformation: DBeaver PostgresSQL, dbt
- Storage: Google Cloud Composer (Apache Airflow)
- Orchestration: Apache Airflow + dbt

## Data Pipeline Architecture
<img width="800" height="300" alt="data architecture pipeline" src="https://github.com/user-attachments/assets/edfcc0df-e377-493d-ad49-292b996b7d1e" />

The file follows these stages:
- Step 1: Upload one month of listing data (following chronological order), census data and LGA mapping into Airflow storage bucket.
- Step 2: Using DBeaver create a Bronze schema and establishh connection to Airflow via [DAG file](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/dbt_project.yml)
- Step 3: Design Medallion Architecture framework with dbt
- Step 4: Create [snapshots](https://github.com/RatanaSovann/data-engineering/tree/main/Airbnb%20%2B%20Census%20Data/snapshots) to handle Slow Changing Dimension (SCD2)
- Step 5: Create datamart and star in gold layer for ad-hoc analysis
- Step 6: Trigger Airflow to update the remaining 11 months of Data manually checking view to ensure robustness of the pipeline.
- Step 7: Perform Ad-Hoc Analysis to answer business questions.

## Step 1: Create staging layer (Bronze Schema) for Data

In this step, I create tables for the three data files Listing, Census and LGA Mapping in DBeaver ensuring correct dtype for each field: [Bronze_Schema.sql](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/Bronze_Schema.sql)

<img width="425" height="448" alt="image" src="https://github.com/user-attachments/assets/de72313a-9f74-4156-bd5f-683ba2fdd6ef" />

## Step 2: Build an Airflow DAG
An Airflow DAG with no set schedule interval reads the data from the storage bucket and loads it into the raw tables within the bronze schema on Postgres. [DAG.py](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/DAG.py)

<img width="425" height="448" alt="image" src="https://github.com/user-attachments/assets/d6ce2607-d750-4386-bf33-9b553d303cd0" />

## Step 3: Design Data Warehouse with DBT (Bronze, Silver, Gold layer)
In this section, the goal is to design a data warehouse architecture in PostgreSQL following the Medallion Architecture framework, which includes Bronze, Silver, and Gold layers. The warehouse will also incorporate dimension and fact tables.


<img width="312" height="226" alt="image" src="https://github.com/user-attachments/assets/c0a9e0bf-af44-44c9-95ec-4e784de35f3b" />


### Create Silver Layer which focuses on three main steps:
- Data cleaning and transformation: This step rectifies inaccuracies, inconsistencies, and missing values to ensure reliable data for analysis.
- Create intermediate tables for Gold Layer: create additional tables s_dim_host, s_dim_listing, s_dim_host_neighbourhood, and s_dim_listing_neighbourhood are created to represent the dimensions of the data. This was designed for later joins in Golder layer.
- Snapshot creation: Cleaned data is captured at specific points in time, enabling tracking of changes and historical comparisons (SCD2)

  
<img width="312" height="226" alt="image" src="https://github.com/user-attachments/assets/7b2beebd-9dab-4f87-a24a-82eebc682ae8" />


[sources.yml](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/models/sources.yml) is created for snapshot to reference which layer to track

The following snapshots were created to track slowly changing dimensions of the data overtime:

<img width="312" height="226" alt="image" src="https://github.com/user-attachments/assets/6aa58d83-df2a-4a0a-b8f2-795cb10d4571" />

<img width="648" height="210" alt="image" src="https://github.com/user-attachments/assets/b357ef4d-a3e9-4beb-8306-4b1d5118ac57" />

### Create Gold Layer consisting of two main components:
- Star Schema: Implements a star schema design with dimension and fact tables. The fact tables include only IDs and key metrics (e.g., price).
- Datamart: Contains materialized views derived from fact and dimension tables to address critical business questions. It also incorporates Slowly Changing Dimensions Type 2 (SCD2) for historical tracking.

<img width="312" height="400" alt="image" src="https://github.com/user-attachments/assets/63b96b66-1140-46fd-86a9-d2397315850e" />

## Step 4: Create Analysis View
### Data Mart:
<img width="300" height="160" alt="image" src="https://github.com/user-attachments/assets/1e16177a-c751-4b91-88aa-3f4dfbc46150" />

The mart folder contains views designed to address key business questions. The following outlines the purpose and structure of each view:

#### 1. [dm_listing_neighbourhood](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/models/gold/mart/dm_listing_neighbourhood.sql): This view provides monthly insights by listing_neighbourhood and month/year, including the following metrics:
- Active listings rate
- Minimum, maximum, median, and average price of active listings
- Number of distinct hosts
- Superhost rate
- Average review score rating for active listings
- Percentage change in active listings
- Percentage change in inactive listings
- Total number of stays
- Average estimated revenue per active listing

<img width="1155" height="621" alt="image" src="https://github.com/user-attachments/assets/1c338ee8-b47e-4717-a279-f00c22b54729" />

Full View: [dm_listing_neighborhood](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/views/dm_listing_neighbourhood_202510270016.csv)

#### 2. [dm_property_type](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/models/gold/mart/dm_property_type.sql): This view provides monthly insights by property type, room type, and accommodates, including the following metrics:
- Active listing rate
- Minimum, maximum, median, and average price of active listings
- Number of distinct hosts
- Superhost rate
- Average review score for active listings
- Total stays for active listings
- Average estimated revenue per active listing
- Month-over-month percentage change in active listings
- Month-over-month percentage change in inactive listings
  
The results are ordered by property type, room type, accommodates, and month/year.

<img width="1168" height="628" alt="image" src="https://github.com/user-attachments/assets/96b36de9-e8aa-4511-ba7c-5edce8622010" />

Full View: [dm_property_type](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/views/dm_property_type_202510270016.csv)

#### 3. [dm_host_neighbourhood](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/models/gold/mart/dm_host_neighbourhood.sql): This view provides monthly insights by host neighbourhood (LGA), including:
- Number of distinct hosts
- Total estimated revenue for active listings
- Estimated revenue per host

The results are ordered by host neighbourhood and month/year.
It aggregates listing data by host, calculates revenue only for active listings, and ensures deduplication of listings per host per month.

<img width="1176" height="618" alt="image" src="https://github.com/user-attachments/assets/aa2fc1f4-75c5-4194-a19e-d9beb2273129" />

Full View: [dm_host_neighbourhood](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/views/dm_host_neighbourhood_202510270014.csv)

## Step 5: Load the remaining Airbnb Dataset (Due to free version limitation)

1. Upload each month’s data to the bucket, then run the Airflow DAG to load one month of Airbnb data at a time.
2. Wait for the data loading to complete — verify the updates in DBeaver to confirm successful loading.
3. Manually trigger the corresponding dbt job to process the newly loaded data.
4. Repeat this process for each subsequent month in chronological order.
5. Ensure that all data is processed sequentially to maintain correct order and data integrity throughout the pipeline.


## Step 6: Ad-Hoc Analysis

The full SQL queries to this section can be found here: [Ad-Hoc_Analysis.sql](https://github.com/RatanaSovann/data-engineering/blob/main/Airbnb%20%2B%20Census%20Data/Ad-Hoc_Analysis.sql)

#### Question 1:

The demographic variations—such as age distribution and household size—between the top three and bottom three LGAs, ranked by estimated revenue per active listing over the past 12 months, show that the median age in the top three LGAs is approximately 40, while in the bottom three it is around 35. Additional differences, including household incomes, are summarized in the table below.

<img width="1000" height="400" alt="image" src="https://github.com/user-attachments/assets/5f08e0e7-eda6-4a2e-8e77-9ac128f54e93" />

#### Question 2:

The table below illustrates the relationship between the median age of a neighbourhood, as reported in Census data, and the revenue generated per active listing within that area. A clear trend emerges from the data, showing that neighbourhoods with higher median ages generally tend to generate greater revenue per listing. For example, LGAs 14100, 15990, and 15350 have median ages of 40 or above, and the revenue associated with listings in these areas is consistently on the higher end compared to neighbourhoods with younger populations. This pattern suggests that age demographics may play a role in influencing the earning potential of listings, potentially reflecting factors such as higher disposable income, more stable household structures, or established communities in older neighbourhoods.

<img width="600" height="700" alt="image" src="https://github.com/user-attachments/assets/4fd1e213-8881-454c-ae31-8c39451c2b37" />

#### Question 3:

The table below highlights the most effective type of listing—considering property type, room type, and number of guests accommodated—for the top five “listing_neighbourhoods” in terms of estimated revenue per active listing. These combinations are associated with the highest number of stays, indicating the characteristics that are most successful in attracting guests within the top-performing neighbourhoods.

<img width="920" height="224" alt="image" src="https://github.com/user-attachments/assets/d13d712b-468a-47d6-83a5-1ba53e9e9240" />

#### Question 4:

For hosts owning multiple listings, the table below displays how many have their properties clustered within a single LGA versus spread across multiple LGAs.

<img width="600" height="140" alt="image" src="https://github.com/user-attachments/assets/d27ad0a6-b32a-432c-a548-0b3e5a2d0bfa" />

Among the hosts owning multiple properties roughly only one third has their property distributed. This makes logical sense as it is harder to manage properties further away from each other.

#### Question 5:

The table below shows whether hosts with a single Airbnb listing have earned enough estimated revenue over the past 12 months to cover the annualised median mortgage repayment in their LGA, and highlights the LGA with the highest share of hosts able to do so.

<img width="880" height="608" alt="image" src="https://github.com/user-attachments/assets/f55badb4-ee7b-4e19-a037-62518fbcde24" />


