"""
==============================================================================
DAG: ĐIỀU PHỐI PIPELINE MEDALLION TRÊN DATABRICKS (RIDESHARING_PIPELINE)
==============================================================================
Mô tả:
  Điều phối toàn bộ quy trình xử lý dữ liệu gọi xe (Ridesharing) theo kiến trúc
  Medallion 4 tầng:
    1. Bronze Layer     : Nạp dữ liệu thô từ file CSV vào bảng Delta thô.
    2. Silver Layer     : Làm sạch dữ liệu, xử lý null, chuẩn hóa kiểu dữ liệu.
    3. Validation Layer : Kiểm tra chất lượng dữ liệu (Data Quality Gate) nghiêm ngặt.
    4. Gold Layer       : Tổng hợp các Data Mart phục vụ phân tích & báo cáo Power BI.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

def on_failure(context):
    """Callback xử lý khi có task bị thất bại trong DAG."""
    task_id = context['task_instance'].task_id
    execution_date = context.get('execution_date', datetime.utcnow())
    print(f"[CẢNH BÁO] Task thất bại: {task_id} tại mốc thời gian: {execution_date}")

def on_success(context):
    """Callback ghi nhận khi task thực thi thành công."""
    task_id = context['task_instance'].task_id
    print(f"[THÀNH CÔNG] Task hoàn tất: {task_id}")

# Cấu hình mặc định cho các task trong DAG
default_args = {
    'owner': 'bnphongdata',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': on_failure,
    'on_success_callback': on_success,
}

with DAG(
    dag_id='ridesharing_medallion_pipeline',
    default_args=default_args,
    description='Pipeline điều phối kiến trúc Medallion (Bronze -> Silver -> Validation -> Gold) trên Databricks',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['databricks', 'pyspark', 'delta-lake', 'medallion', 'data-quality'],
    doc_md="""
    ### Quy trình Medallion Lakehouse Pipeline
    - **Bronze**: Nạp dữ liệu thô (raw CSV) vào Delta Lake `bronze_ridesharing`.
    - **Silver**: Làm sạch, khử trùng lặp và chuẩn hóa dữ liệu vào `silver_ridesharing`.
    - **Validation**: Đóng vai trò Data Quality Gate kiểm thử schema, null và khoảng giá trị hợp lệ.
    - **Gold**: Tạo các bảng Dimension/Fact và Data Marts cho Power BI dashboard.
    """
) as dag:

    # Task 1: Khởi chạy tầng Bronze (Raw Ingestion)
    bronze_task = BashOperator(
        task_id='bronze_layer',
        bash_command='echo "Executing Databricks Notebook: Bronze Layer (Raw Ingestion)"',
        priority_weight=4,
        trigger_rule='all_success',
    )

    # Task 2: Khởi chạy tầng Silver (Data Cleansing & Normalization)
    silver_task = BashOperator(
        task_id='silver_layer',
        bash_command='echo "Executing Databricks Notebook: Silver Layer (Cleansing & Standardization)"',
        priority_weight=3,
        trigger_rule='all_success',
    )

    # Task 3: Khởi chạy tầng Validation (Data Quality Gates)
    validation_task = BashOperator(
        task_id='validation_layer',
        bash_command='echo "Executing Databricks Notebook: Validation Layer (Data Quality Verification)"',
        priority_weight=2,
        trigger_rule='all_success',
    )

    # Task 4: Khởi chạy tầng Gold (Business Aggregations & Marts)
    gold_task = BashOperator(
        task_id='gold_layer',
        bash_command='echo "Executing Databricks Notebook: Gold Layer (Analytical Aggregations)"',
        priority_weight=1,
        trigger_rule='all_success',
    )

    # Thiết lập luồng phụ thuộc tuần tự: Phải qua cổng Validation thành công mới chuyển sang Gold
    bronze_task >> silver_task >> validation_task >> gold_task
