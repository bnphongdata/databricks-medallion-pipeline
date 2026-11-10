# Databricks notebook source
# ==============================================================================
# TẦNG SILVER: LÀM SẠCH VÀ CHUẨN HÓA DỮ LIỆU (CLEANSING & STANDARDIZATION)
# ==============================================================================
# Mục tiêu:
#   1. Đọc dữ liệu từ bảng Delta tầng Bronze.
#   2. Khử trùng lặp bản ghi (Deduplication) dựa trên trip_id.
#   3. Loại bỏ bản ghi thiếu các khóa định danh chính (trip_id, rider_id, driver_id).
#   4. Điền giá trị mặc định cho các cột chuỗi và số bị thiếu (Imputation).
#   5. Lọc bỏ các giá trị không hợp lệ (cước âm, khoảng cách <= 0, thời gian bay <= 0).
#   6. Áp dụng kỹ thuật điền khuyết số liệu thống kê (Mean cho tọa độ, Median cho giá cước).
#   7. Chuẩn hóa chuỗi văn bản về chữ thường (lowercase) để phục vụ join và phân tích.
#   8. Ghi dữ liệu sạch vào bảng Delta tầng Silver (silver_ridesharing).
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# 1. Khởi tạo SparkSession và đọc dữ liệu từ bảng Bronze Delta Lake
spark = SparkSession.builder.appName("Silver_Layer_Cleansing").getOrCreate()
silver_df = spark.read.format("delta").table("ride_catalogue.default.bronze_ridesharing")
print(f"Số dòng ban đầu từ tầng Bronze: {silver_df.count()}")

# COMMAND ----------

# 2. Khử trùng lặp dữ liệu theo khóa chính của chuyến đi (trip_id)
silver_df = silver_df.dropDuplicates(["trip_id"])
print(f"Số dòng sau khi loại bỏ trùng lặp: {silver_df.count()}")

# COMMAND ----------

# 3. Loại bỏ các bản ghi thiếu các trường định danh bắt buộc
silver_df = silver_df.dropna(subset=["trip_id", "rider_id", "driver_id"])

# 4. Điền giá trị mặc định (Imputation) cho các trường phân loại và thông tin liên hệ bị null
silver_df = silver_df.fillna({
    "cancellation_reason": "No_Reason",
    "promo_code": "No_Promo",
    "promo_discount": 0.0,
    "os_platform": "Unknown",
    "rider_name": "Unknown",
    "rider_email": "Unknown",
    "payment_type": "Unknown",
    "surge_zone": "Unknown",
    "driver_name": "Unknown",
    "rider_phone": "Unknown",
    "driver_phone": "Unknown",
    "pickup_location": "Unknown",
    "dropoff_location": "Unknown",
    "vehicle_type": "Unknown",
    "vehicle_plate": "Unknown",
    "ride_status": "Unknown",
    "dropoff_datetime": "Unknown",
    "pickup_datetime": "Unknown",
    "tip": 0.0
})

# COMMAND ----------

# 5. Lọc bỏ các bản ghi không hợp lệ về mặt nghiệp vụ (giá cước, khoảng cách và thời gian phải dương)
silver_df = silver_df.filter(
    (col("total_fare") > 0) & 
    (col("distance_km") > 0) & 
    (col("trip_duration_mins") > 0)
)

# COMMAND ----------

# 6. Chuẩn hóa các trường phân loại chính về chữ thường (lowercase)
silver_df = (
    silver_df
    .withColumn("vehicle_type", lower(col("vehicle_type")))
    .withColumn("payment_type", lower(col("payment_type")))
    .withColumn("ride_status", lower(col("ride_status")))
)

# COMMAND ----------

# 7. Lọc khoảng điểm đánh giá hợp lệ của tài xế và hành khách (từ 1 đến 5 sao)
silver_df = silver_df.filter(
    col("driver_rating").between(1, 5) & 
    col("rider_rating").between(1, 5)
)

# COMMAND ----------

# 8. Xử lý giá trị tọa độ bị khuyết: Tính giá trị trung bình (Mean) và điền vào ô trống
pickup_lat_mean  = silver_df.select(mean("pickup_lat")).collect()[0][0]
pickup_lng_mean  = silver_df.select(mean("pickup_lng")).collect()[0][0]
dropoff_lat_mean = silver_df.select(mean("dropoff_lat")).collect()[0][0]
dropoff_lng_mean = silver_df.select(mean("dropoff_lng")).collect()[0][0]

silver_df = silver_df.fillna({
    "pickup_lat": pickup_lat_mean,
    "pickup_lng": pickup_lng_mean,
    "dropoff_lat": dropoff_lat_mean,
    "dropoff_lng": dropoff_lng_mean
})

# Làm tròn tọa độ địa lý đến 6 chữ số thập phân (độ chính xác ~0.1m)
silver_df = (
    silver_df
    .withColumn("pickup_lat", round(col("pickup_lat"), 6))
    .withColumn("pickup_lng", round(col("pickup_lng"), 6))
    .withColumn("dropoff_lat", round(col("dropoff_lat"), 6))
    .withColumn("dropoff_lng", round(col("dropoff_lng"), 6))
)

# COMMAND ----------

# 9. Xử lý giá trị cước cơ bản và hệ số tăng giá:
#    - Dùng Trung vị (Median) cho cước cơ bản để chống ảnh hưởng bởi giá trị ngoại lai (outliers)
#    - Dùng Trung bình (Mean) cho hệ số tăng giá (Surge Multiplier)
base_fare_median = float(silver_df.approxQuantile("base_fare", [0.5], 0.0)[0])
surge_multiplier_mean = float(silver_df.select(mean("surge_multiplier")).collect()[0][0])

silver_df = silver_df.fillna({
    "base_fare": base_fare_median, 
    "surge_multiplier": surge_multiplier_mean
})

# Làm tròn hệ số tăng giá đến 1 chữ số thập phân (ví dụ: 1.2x, 1.5x)
silver_df = silver_df.withColumn("surge_multiplier", round(col("surge_multiplier"), 1))

# COMMAND ----------

# 10. Chuẩn hóa đồng bộ các trường chuỗi còn lại về dạng chữ thường
silver_df = (
    silver_df
    .withColumn("surge_zone", lower(col("surge_zone")))
    .withColumn("pickup_location", lower(col("pickup_location")))
    .withColumn("dropoff_location", lower(col("dropoff_location")))
    .withColumn("os_platform", lower(col("os_platform")))
    .withColumn("promo_code", lower(col("promo_code")))
)

# COMMAND ----------

# 11. Lưu trữ dữ liệu đã làm sạch vào bảng Delta tầng Silver
TARGET_SILVER_TABLE = "ride_catalogue.default.silver_ridesharing"

silver_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_SILVER_TABLE)
print(f"-> Hoàn tất tầng Silver. Đã lưu {silver_df.count()} dòng dữ liệu sạch vào: {TARGET_SILVER_TABLE}")
