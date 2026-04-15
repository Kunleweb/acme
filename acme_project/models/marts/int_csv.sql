with base as (
    select
        -- IDs (cast to integers)
        try_to_number(STORE_ID) as store_id,
        try_to_number(PRODUCT_ID) as product_id,
        try_to_number(CUSTOMER_ID) as customer_id,
        try_to_number(TRANSACTION_ID) as transaction_id,

        -- Core measures
        try_to_number(QUANTITY) as quantity,

        try_to_number(UNIT_PRICE, 10, 2) as unit_price,
        try_to_number(TOTAL_AMOUNT, 10, 2) as total_amount,

        -- Time dimension
        try_to_timestamp(TRANSACTION_TIMESTAMP) as transaction_time,

        -- Descriptive fields
        PRODUCT_NAME as product_name,
        CATEGORY as category,
        PAYMENT_METHOD as payment_method,

        -- Airbyte / metadata (keep as string/variant safe storage)
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_META,
        _AIRBYTE_GENERATION_ID,
        _AB_SOURCE_FILE_URL,
        _AB_SOURCE_FILE_LAST_MODIFIED

    from {{ ref('stg_csv') }}

)

select *
from base