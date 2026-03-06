-- Find the demographic differences (e.g., age group distribution, household size) between the top 3 performing and lowest 3 performing LGAs based on estimated revenue per active listing over the last 12 months?

WITH aggregated_neighbourhood AS (
    SELECT
        dn.listing_neighbourhood,
        ROUND(AVG(dn.avg_estimated_revenue_per_active_listing), 2) AS avg_estimated_revenue_per_active_listing
    FROM gold.dm_listing_neighbourhood dn
    GROUP BY dn.listing_neighbourhood
),
lga_revenue AS (
    SELECT 
        an.listing_neighbourhood,
        dl.lga_code,
        an.avg_estimated_revenue_per_active_listing
    FROM aggregated_neighbourhood an
    JOIN gold.g_dim_listing_neighbourhood dl 
        ON LOWER(an.listing_neighbourhood) = LOWER(dl.listing_neighbourhood)
),
top_bottom_lgas AS (
    SELECT *
    FROM (
        SELECT 
            lga_code,
            listing_neighbourhood,
            ROUND(AVG(avg_estimated_revenue_per_active_listing), 2) AS avg_revenue_per_active_listing
        FROM lga_revenue
        GROUP BY lga_code, listing_neighbourhood
        ORDER BY avg_revenue_per_active_listing DESC
        LIMIT 3
    ) AS top_lgas
    UNION ALL
    SELECT *
    FROM (
        SELECT 
            lga_code,
            listing_neighbourhood,
            ROUND(AVG(avg_estimated_revenue_per_active_listing), 2) AS avg_revenue_per_active_listing
        FROM lga_revenue
        GROUP BY lga_code, listing_neighbourhood
        ORDER BY avg_revenue_per_active_listing ASC
        LIMIT 3
    ) AS bottom_lgas
),
demographics AS (
    SELECT 
        tb.lga_code,
        tb.listing_neighbourhood,
        tb.avg_revenue_per_active_listing,
        cg.median_age,
        cg.average_persons_per_bedroom,
        cg.median_household_income,
        cg.average_household_size,
        cg1.age_20_24_p,
        cg1.age_25_34_p,
        cg1.age_35_44_p,
        cg1.age_45_54_p
    FROM top_bottom_lgas tb
	JOIN gold.g_census_g02 cg 
    	ON CAST(tb.lga_code AS INTEGER) = cg.lga_code
	JOIN gold.g_census_g01 cg1
    	ON CAST(tb.lga_code AS INTEGER) = cg1.lga_code
)
SELECT * 
FROM demographics
ORDER BY avg_revenue_per_active_listing DESC;


-- Investigate correlation between the median age of a neighbourhood (from Census data) and the revenue generated per active listing in that neighbourhood?

WITH aggregated_neighbourhood AS (
    SELECT
        dn.listing_neighbourhood,
        ROUND(AVG(dn.avg_estimated_revenue_per_active_listing), 2) AS avg_estimated_revenue_per_active_listing
    FROM gold.dm_listing_neighbourhood dn
    GROUP BY dn.listing_neighbourhood
),
lga_revenue AS (
    SELECT 
        an.listing_neighbourhood,
        dl.lga_code,
        an.avg_estimated_revenue_per_active_listing
    FROM aggregated_neighbourhood an
    JOIN gold.g_dim_listing_neighbourhood dl 
        ON LOWER(an.listing_neighbourhood) = LOWER(dl.listing_neighbourhood)
),
lga_age_revenue AS (
    SELECT
        lr.lga_code,
        ROUND(SUM(lr.avg_estimated_revenue_per_active_listing), 2) AS avg_revenue_per_active_listing,
        cg.median_age
    FROM lga_revenue lr
    JOIN gold.g_census_g02 cg 
    	ON CAST(lr.lga_code AS INTEGER) = cg.lga_code
	GROUP BY lr.lga_code, cg.median_age
)
SELECT 
    lga_code,
    avg_revenue_per_active_listing,
    median_age
FROM lga_age_revenue
ORDER BY avg_revenue_per_active_listing;



-- Find the best type of listing (property type, room type, accommodates) for each of the top 5 performing neighbourhoods based on estimated revenue per active listing with the highest number of stays.

WITH active_listings AS (
    SELECT
        fl.id,
        fl.listing_id,
        fl.host_id,
        fl.price,
        fl.availability_30,
        l.has_availability,
        dln.listing_neighbourhood,
        l.property_type,
        l.room_type,
        fl.accommodates,
        fl.scraped_date
    FROM gold.g_fact_listing fl
    JOIN gold.g_dim_listing_neighbourhood dln
        ON fl.listing_neighbourhood = dln.listing_neighbourhood
    JOIN gold.g_dim_listing l
        ON fl.id = l.id
    WHERE l.has_availability = 'T'
),
estimated_revenue AS (
    SELECT
        listing_neighbourhood,
        AVG((30 - availability_30) * price) AS avg_estimated_revenue
    FROM active_listings
    GROUP BY listing_neighbourhood
),
top_5_neighbourhoods AS (
    SELECT listing_neighbourhood
    FROM estimated_revenue
    ORDER BY avg_estimated_revenue DESC
    LIMIT 5
),
stays_by_type AS (
    SELECT
        al.listing_neighbourhood,
        al.property_type,
        al.room_type,
        al.accommodates,
        SUM(30 - al.availability_30) AS total_stays
    FROM active_listings al
    WHERE al.listing_neighbourhood IN (SELECT listing_neighbourhood FROM top_5_neighbourhoods)
    GROUP BY al.listing_neighbourhood, al.property_type, al.room_type, al.accommodates
),
best_listing_types AS (
    SELECT
        sbt.listing_neighbourhood,
        sbt.property_type,
        sbt.room_type,
        sbt.accommodates,
        sbt.total_stays,
        ROW_NUMBER() OVER (
            PARTITION BY sbt.listing_neighbourhood
            ORDER BY sbt.total_stays DESC
        ) AS rn
    FROM stays_by_type sbt
)
SELECT
    blt.listing_neighbourhood,
    blt.property_type,
    blt.room_type,
    blt.accommodates,
    blt.total_stays
FROM best_listing_types blt
WHERE blt.rn = 1
ORDER BY blt.listing_neighbourhood;


-- Investigate whether hosts with multiple listings tend to concentrate their listings in a single LGA or distribute them across multiple LGAs

WITH host_listings AS (
    SELECT 
        fl.host_id,
        dn.lga_code,
        fl.listing_id
    FROM gold.g_fact_listing fl
    JOIN gold.g_dim_listing dl 
        ON fl.listing_id = dl.listing_id
    JOIN gold.g_dim_listing_neighbourhood dn 
        ON dl.listing_neighbourhood = dn.listing_neighbourhood
),
multiple_host_listings AS (
    SELECT 
        hl.host_id,
        COUNT(DISTINCT hl.listing_id) AS total_listings
    FROM host_listings hl
    GROUP BY hl.host_id
    HAVING COUNT(DISTINCT hl.listing_id) > 1
),
lga_counts AS (
    SELECT 
        hl.host_id,
        hl.lga_code,
        COUNT(hl.listing_id) AS listings_per_lga
    FROM host_listings hl
    JOIN multiple_host_listings mhl 
        ON hl.host_id = mhl.host_id
    GROUP BY hl.host_id, hl.lga_code
),
concentrated_hosts AS (
    SELECT host_id
    FROM lga_counts
    GROUP BY host_id
    HAVING COUNT(DISTINCT lga_code) = 1
),
distributed_hosts AS (
    SELECT host_id
    FROM lga_counts
    GROUP BY host_id
    HAVING COUNT(DISTINCT lga_code) > 1
)
SELECT 
    COUNT(DISTINCT ch.host_id) AS total_concentrated_hosts,
    COUNT(DISTINCT dh.host_id) AS total_distributed_hosts
FROM concentrated_hosts ch, distributed_hosts dh;


-- For hosts with only one listing, determine the percentage of those listings that generate enough estimated revenue to cover the average annual mortgage repayment for that LGA based on Census data.

WITH single_listing_hosts AS (
    SELECT
        dl.host_id,
        dl.listing_neighbourhood
    FROM 
        gold.g_dim_listing dl
    JOIN 
        gold.g_fact_listing fl ON dl.listing_id = fl.listing_id
    GROUP BY 
        dl.host_id, dl.listing_neighbourhood
    HAVING 
        COUNT(fl.listing_id) = 1
),

estimated_revenue AS (
    SELECT
        slh.host_id,
        dn.lga_code,
        SUM(dln.avg_estimated_revenue_per_active_listing) AS total_estimated_revenue
    FROM 
        single_listing_hosts slh
    JOIN 
        gold.g_dim_listing_neighbourhood dn ON slh.listing_neighbourhood = dn.listing_neighbourhood
    JOIN 
        gold.dm_listing_neighbourhood dln ON slh.listing_neighbourhood = dln.listing_neighbourhood
    GROUP BY 
        slh.host_id, dn.lga_code
),

annual_mortgage AS (
    SELECT
        cm.lga_code,
        cm.median_mortgage_repay * 12 AS annual_mortgage_repay
    FROM 
        gold.g_census_g02 cm
),

revenue_coverage AS (
    SELECT
        er.lga_code,
        COUNT(er.host_id) AS total_hosts,
        SUM(CASE WHEN er.total_estimated_revenue >= am.annual_mortgage_repay THEN 1 ELSE 0 END) AS hosts_that_can_cover
    FROM 
        estimated_revenue er
    JOIN 
        annual_mortgage am ON cast(er.lga_code as INTEGER) = am.lga_code
    GROUP BY 
        er.lga_code
)

SELECT 
    dn.lga_name AS lga,
    rc.total_hosts,
    rc.hosts_that_can_cover,
    ROUND((rc.hosts_that_can_cover::DECIMAL / rc.total_hosts) * 100, 2) AS percentage_can_cover
FROM 
    revenue_coverage rc
JOIN 
    gold.g_dim_listing_neighbourhood dn ON rc.lga_code = dn.lga_code
ORDER BY 
    percentage_can_cover DESC;
