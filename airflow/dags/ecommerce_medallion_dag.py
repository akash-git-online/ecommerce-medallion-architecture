from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

datasets = [
    "olist_customers_dataset",
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_geolocation_dataset",
    "product_category_name_translation",
]

with DAG(
    dag_id="ecommerce_medallion_pipeline",
    schedule=None,
    start_date=datetime(2026, 6, 24),
    catchup=False,
    # Cap concurrent bronze tasks so parallel Spark jobs don't OOM the
    # jupyter container. Raise gradually while watching `docker stats`.
    max_active_tasks=2,
) as dag:

    upload_to_landing = BashOperator(
        task_id="upload_to_landing",
        bash_command="docker exec jupyter python /home/jovyan/ingestion/upload_to_landing.py"
    )

    # One bronze task per dataset, each depending only on the upload step.
    # This fans them out in parallel (bounded by max_active_tasks above)
    # rather than chaining them sequentially.
    for dataset in datasets:
        bronze_task = BashOperator(
            task_id=f"landing_to_bronze_{dataset}",
            bash_command=f"docker exec jupyter python /home/jovyan/work/landing_to_bronze.py {dataset}",
        )
        upload_to_landing >> bronze_task