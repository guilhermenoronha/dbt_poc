{# in dbt Develop #}

{% set old_etl_relation=ref('customer_orders') -%}

{{ log("OLD ETL RELATION: " ~ old_etl_relation, info=true) }}

{% set dbt_relation=ref('fct_customers_orders') %}

{{ log("DBT RELATION: " ~ dbt_relation, info=true) }}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="order_id"
) }}



