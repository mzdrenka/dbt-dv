with source_data as (

    select
        ID,
        NAME
    from {{ source('seeds', 'raw_customers') }}

),

hashed as (

    select
        {{ dbt_utils.generate_surrogate_key(['ID']) }} as CUSTOMER_ID_HASH,
        ID as CUSTOMER_ID,
        NAME as CUSTOMER_NAME,
        current_timestamp as LOAD_DATETIME,
        'RAW_CUSTOMERS' as RECORD_SOURCE

    from source_data

)

select * from hashed
