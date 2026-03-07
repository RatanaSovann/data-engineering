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

- ## Technologies

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

## Step 4: Business Analysis
###Data Mart:



The mart folder contains views designed to address key business questions. The following outlines the purpose and structure of each view:

1. [dm_listing_neighbourhood](): This view provides monthly insights by listing_neighbourhood and month/year, including the following metrics:

- Active listings rate
- Minimum, maximum, median, and average price of active listings
- Number of distinct hosts
- Superhost rate
- Average review score rating for active listings
- Percentage change in active listings
- Percentage change in inactive listings
- Total number of stays
- Average estimated revenue per active listing









