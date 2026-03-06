{{
    config(
        unique_key='lga_id',
        alias='lga_suburb'
    )
}}

select
{{ dbt_utils.generate_surrogate_key(['lga_name', 'suburb_name']) }} as id
, *
from {{ source('raw', 'nsw_lga_suburb') }}