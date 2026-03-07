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
- Step 2: Using DBeaver create a Bronze schema and establishh connection to Airflow via [DAG file](
