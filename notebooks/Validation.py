# Databricks notebook source
# ==============================================================================
# TẦNG KIỂM THỬ: CỔNG KIỂM SOÁT CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY GATES)
# ==============================================================================
# Mục tiêu:
#   Đóng vai trò là "Circuit Breaker" giữa tầng Silver và Gold:
#   1. Đảm bảo dữ liệu trong bảng Silver đáp ứng đầy đủ 8 tiêu chuẩn chất lượng.
#   2. Nếu bất kỳ bài kiểm tra nào thất bại (Assertion Error), pipeline sẽ NGẮT NGAY
#      để ngăn chặn việc truyền dữ liệu bẩn/lỗi sang các bảng báo cáo tầng Gold.
#
# Danh mục 8 bài kiểm tra chất lượng dữ liệu:
#   - Check 1: Kiểm tra bảng không được rỗng (Row count > 0).
#   - Check 2: Kiểm tra tính duy nhất của khóa chính (0 bản ghi trùng lặp trip_id).
#   - Check 3: Kiểm tra cước phí không được âm (No negative total fare).
#   - Check 4: Kiểm tra tính toàn vẹn số lượng cột (Đủ chính xác 33 cột).
#   - Check 5: Kiểm tra kiểu dữ liệu của các trường cốt lõi (Schema Type Enforcement).
#   - Check 6: Kiểm tra không được chứa giá trị null ở các trường quan trọng (PK/FKs).
#   - Check 7: Kiểm tra khoảng giá trị hợp lệ của điểm đánh giá rating (1.0 đến 5.0).
#   - Check 8: Kiểm tra tính dương của quãng đường và thời gian (distance & duration > 0).
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# Khởi tạo SparkSession và nạp bảng Silver từ Delta Lake
spark = SparkSession.builder.appName("Data_Quality_Validation").getOrCreate()
SOURCE_TABLE = "ride_catalogue.default.silver_ridesharing"
val_df = spark.read.format("delta").table(SOURCE_TABLE)

print(f"====================================================================")
print(f"BẮT ĐẦU KIỂM TRA CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY GATES)")
print(f"Bảng kiểm tra: {SOURCE_TABLE}")
print(f"====================================================================")

# COMMAND ----------

# CHECK 1: Kiểm tra số lượng bản ghi (Row Count Check)
row_count = val_df.count()
assert row_count > 0, f"[LỖI DQ #1]: Bảng {SOURCE_TABLE} rỗng, không có dữ liệu!"
print(f"[PASSED] Check 1 - Số lượng bản ghi hợp lệ: {row_count:,} dòng.")

# COMMAND ----------

# CHECK 2: Kiểm tra tính duy nhất của khóa chính trip_id (Duplicate Key Check)
duplicate_count = val_df.count() - val_df.dropDuplicates(["trip_id"]).count()
assert duplicate_count == 0, f"[LỖI DQ #2]: Phát hiện {duplicate_count} bản ghi bị trùng lặp khóa trip_id!"
print(f"[PASSED] Check 2 - Tính duy nhất khóa chính: Không có bản ghi trùng lặp trip_id.")

# COMMAND ----------

# CHECK 3: Kiểm tra giá cước tổng không được âm (Non-negative Total Fare Check)
negative_fares = val_df.filter(col("total_fare") < 0).count()
assert negative_fares == 0, f"[LỖI DQ #3]: Phát hiện {negative_fares} bản ghi có tổng cước total_fare < 0!"
print(f"[PASSED] Check 3 - Giá trị cước: Toàn bộ cước phí đều >= 0.")

# COMMAND ----------

# CHECK 4: Kiểm tra cấu trúc số lượng cột (Column Count Integrity Check)
expected_cols = 33
actual_cols = len(val_df.columns)
assert actual_cols == expected_cols, (
    f"[LỖI DQ #4]: Cấu trúc số cột không đúng! Yêu cầu {expected_cols} cột, thực tế có {actual_cols} cột."
)
print(f"[PASSED] Check 4 - Cấu trúc cột: Đầy đủ chính xác {actual_cols}/{expected_cols} cột.")

# COMMAND ----------

# CHECK 5: Kiểm tra kiểu dữ liệu chuẩn của các trường cốt lõi (Schema Data Type Validation)
expected_schema = {
    "trip_id": "string",
    "rider_id": "string",
    "driver_id": "string",
    "trip_duration_mins": "int",
    "pickup_lat": "double",
    "pickup_lng": "double",
    "dropoff_lat": "double",
    "dropoff_lng": "double",
    "distance_km": "double",
    "base_fare": "double",
    "surge_multiplier": "double",
    "tip": "double",
    "total_fare": "double",
    "driver_rating": "double",
    "rider_rating": "double",
    "promo_discount": "double"
}

actual_schema = dict(val_df.dtypes)

for col_name, expected_type in expected_schema.items():
    assert actual_schema[col_name] == expected_type, (
        f"[LỖI DQ #5]: Sai kiểu dữ liệu tại cột '{col_name}'! "
        f"Kỳ vọng: '{expected_type}', Thực tế: '{actual_schema[col_name]}'."
    )

print(f"[PASSED] Check 5 - Kiểm tra Schema: 16 trường cốt lõi đều đúng kiểu dữ liệu.")

# COMMAND ----------

# CHECK 6: Kiểm tra không chứa giá trị null ở các trường khóa bắt buộc (Critical Null Check)
critical_cols = ["trip_id", "rider_id", "driver_id"]
for col_name in critical_cols:
    null_count = val_df.filter(col(col_name).isNull()).count()
    assert null_count == 0, f"[LỖI DQ #6]: Cột quan trọng '{col_name}' phát hiện {null_count} giá trị NULL!"

print(f"[PASSED] Check 6 - Tính toàn vẹn khóa: Không có giá trị NULL trong {critical_cols}.")

# COMMAND ----------

# CHECK 7: Kiểm tra khoảng đánh giá rating hợp lệ (Rating Range Boundary Check: 1.0 - 5.0)
invalid_ratings = val_df.filter(
    (col("driver_rating") < 1) | (col("driver_rating") > 5) |
    (col("rider_rating") < 1) | (col("rider_rating") > 5)
).count()
assert invalid_ratings == 0, f"[LỖI DQ #7]: Phát hiện {invalid_ratings} điểm rating nằm ngoài khoảng 1 - 5 sao!"
print(f"[PASSED] Check 7 - Điểm đánh giá: Tất cả rating tài xế và hành khách đều trong khoảng 1 - 5 sao.")

# COMMAND ----------

# CHECK 8: Kiểm tra giá trị dương đối với khoảng cách và thời gian chuyến đi (Positive Metric Check)
invalid_metrics = val_df.filter(
    (col("distance_km") <= 0) | (col("trip_duration_mins") <= 0)
).count()
assert invalid_metrics == 0, f"[LỖI DQ #8]: Phát hiện {invalid_metrics} bản ghi có quãng đường hoặc thời gian <= 0!"
print(f"[PASSED] Check 8 - Chỉ số đo lường: Toàn bộ distance_km và trip_duration_mins đều > 0.")

# COMMAND ----------

print(f"====================================================================")
print(f"TẤT CẢ 8 CỔNG KIỂM TRA CHẤT LƯỢNG DỮ LIỆU ĐỀU ĐẠT CHUẨN (100% PASSED)!")
print(f"Dữ liệu tầng Silver hoàn toàn đủ điều kiện chuyển giao sang tầng Gold.")
print(f"====================================================================")
