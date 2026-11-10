# Databricks notebook source
# ==============================================================================
# TẦNG BRONZE: NẠP DỮ LIỆU THÔ VÀO DELTA LAKE (RAW INGESTION)
# ==============================================================================
# Mục tiêu:
#   1. Đọc dữ liệu gọi xe gốc (raw CSV) từ Unity Catalog Volume hoặc Cloud Storage.
#   2. Giữ nguyên trạng dữ liệu (Raw Data Preservation), chưa thực hiện lọc hay sửa đổi.
#   3. Ghi dữ liệu vào bảng Delta định dạng Bronze (ride_catalogue.default.bronze_ridesharing).
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Khởi tạo hoặc lấy phiên làm việc SparkSession hiện tại trên Databricks
spark = SparkSession.builder.appName("Bronze_Layer_Ingestion").getOrCreate()

# Đường dẫn tệp dữ liệu thô (có thể đọc từ Unity Catalog Volume hoặc DBFS)
RAW_DATA_PATH = "/Volumes/ride_catalogue/default/ride_volume/ridesharing_raw.csv"

# Đọc dữ liệu CSV gốc với header và tự động suy luận kiểu dữ liệu
bronze_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(RAW_DATA_PATH)
)

print(f"-> Đã nạp thành công {bronze_df.count()} dòng dữ liệu thô vào tầng Bronze.")

# COMMAND ----------

# Lưu trữ dữ liệu thô vào bảng Delta Lake ở chế độ ghi đè (Overwrite)
TARGET_BRONZE_TABLE = "ride_catalogue.default.bronze_ridesharing"

bronze_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_BRONZE_TABLE)
print(f"-> Đã lưu dữ liệu vào bảng Delta: {TARGET_BRONZE_TABLE}")

# COMMAND ----------

# Hiển thị mẫu dữ liệu tầng Bronze để kiểm tra
bronze_df.display()

# COMMAND ----------

# In cấu trúc Schema của bảng Bronze
bronze_df.printSchema()
