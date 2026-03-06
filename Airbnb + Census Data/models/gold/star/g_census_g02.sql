{{
    config(
        unique_key='lga_code_2016',
        alias='g_census_g02'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('b_census_g02') }}
),
cleaned_data AS (
  SELECT
    CAST(REPLACE(LGA_CODE_2016, 'LGA', '') AS INT) AS lga_code,
    CAST(Median_age_persons AS INT) AS median_age,
    CAST(Median_mortgage_repay_monthly AS INT) AS median_mortgage_repay,
    CAST(Median_tot_prsnl_inc_weekly AS INT) AS median_personal_income,
    CAST(Median_rent_weekly AS INT) AS median_rent,
    CAST(Median_tot_fam_inc_weekly AS INT) AS median_family_income,
    CAST(Average_num_psns_per_bedroom AS DECIMAL(3, 1)) AS average_persons_per_bedroom,
    CAST(Median_tot_hhd_inc_weekly AS INT) AS median_household_income,
    CAST(Average_household_size AS DECIMAL(3, 1)) AS average_household_size
  FROM source
)

SELECT * FROM cleaned_data
