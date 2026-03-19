#  Youtube Trending Insight

<img width="843" height="254" alt="image" src="https://github.com/user-attachments/assets/51146397-558a-4856-913c-fe816fbf1f96" />

## Objective

This project involve analysing a large dataset using a Data Lakehouse with Snowflakes. It involves:
1. Analyse YouTube trending data using Snowflake and Azure cloud storage
2. Clean and preprocess data (duplicates, missing values, inconsistent formats)
3. Perform SQL analysis to uncover trending patterns and answer research questions
4. Identify top-performing categories by engagement and speed to trend across countries
5. Generate actionable insights and recommendations for YouTube content strategy

## Dataset Used

A dataset with a daily record of the top trending YouTube videos has been extracted through the Youtube API and made available on the Kaggle (https://www.kaggle.com/rsrishav/youtube-trending-video-dataset)

The dataset used in this analysis consists of two types of files: CSV files containing trending video information and JSON files providing category titles for each country.
After uploading to Azure, the data was ingested into Snowflake and stored in internal tables. Data cleaning included handling null values and ensuring consistency. The CSV and JSON files were then merged into a consolidated table "table_youtube_final", with additional preprocessing such as creating unique identifiers and removing duplicates, forming a clean dataset ready for analysis.

## Technologies used:

- Language: SQL
- Extraction and transformation: Snowflake SQL
- Storage: Azure Blob Storage
- Data Warehouse: Snowflake Data Lakehouse
- Data Processing: Snowflake (internal tables, data cleaning, merging)

## Overview of Data Pre-processing:
<img width="1125" height="793" alt="image" src="https://github.com/user-attachments/assets/f8668528-e090-4821-b65d-73d556408caa" />

The final table contains the following 13 fields: Video ID, Title, Published Date, Channel ID, Channel Title, Category ID, Category Title, Trending Date, View Count, Likes, Dislikes, Comment Count, Country.
