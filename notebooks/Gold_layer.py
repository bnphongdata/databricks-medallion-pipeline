# Databricks notebook source
# ==============================================================================
# TẦNG GOLD: TỔNG HỢP VÀ MÔ HÌNH HÓA KINH DOANH (ANALYTICAL MARTS)
# ==============================================================================
# Mục tiêu:
#   1. Đọc dữ liệu sạch đã qua kiểm định chất lượng từ bảng Silver.
#   2. Xử lý chuẩn hóa thời gian đa định dạng (Multi-format timestamp resolution):
#      Sử dụng coalesce kết hợp try_to_timestamp hỗ trợ 8 định dạng ngày giờ khác nhau.
#   3. Trích xuất các thuộc tính thời gian (pickup_hour, pickup_date) phục vụ phân tích xu hướng.
#   4. Xây dựng 11 bảng tổng hợp chuyên sâu (Data Marts) phục vụ Dashboard Power BI:
#      - driver_revenue_summary    : Doanh thu theo từng tài xế.
#      - rider_trip_summary         : Tần suất chuyến đi theo hành khách.
#      - driver_rating_summary      : Đánh giá chất lượng phục vụ của tài xế.
#      - vehicle_revenue_summary    : Hiệu quả kinh doanh theo loại phương tiện.
#      - payment_revenue_summary    : Phân bổ doanh thu theo hình thức thanh toán.
#      - peak_hours_summary         : Xác định các khung giờ cao điểm trong ngày.
#      - cancellation_summary       : Phân tích nguyên nhân và tỷ lệ hủy chuyến.
#      - top_pickup_summary         : Các điểm đón khách tập trung nhiều nhất.
#      - top_dropoff_summary        : Các điểm trả khách phổ biến nhất.
#      - platform_summary           : Phân tích hành vi theo hệ điều hành (iOS, Android).
#      - surge_zone_summary         : Đo lường mức độ nhu cầu tại các vùng tăng giá.
#   5. Ghi toàn bộ kết quả vào các bảng Delta Lake tầng Gold.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# 1. Khởi tạo SparkSession và đọc dữ liệu từ bảng Silver
spark = SparkSession.builder.appName("Gold_Layer_Analytics").getOrCreate()
SOURCE_SILVER_TABLE = "ride_catalogue.default.silver_ridesharing"
gold_df = spark.read.format("delta").table(SOURCE_SILVER_TABLE)

# COMMAND ----------

# 2. Xử lý chuẩn hóa định dạng ngày giờ phức tạp (Hỗ trợ 8 dạng định dạng khác nhau)
gold_df.createOrReplaceTempView("gold_temp")

gold_df = spark.sql("""
    SELECT *,
    COALESCE(
        try_to_timestamp(pickup_datetime, 'MM-dd-yyyy'),
        try_to_timestamp(pickup_datetime, 'yyyy/MM/dd HH:mm'),
        try_to_timestamp(pickup_datetime, 'MMMM dd, yyyy'),
        try_to_timestamp(pickup_datetime, 'MM/dd/yyyy'),
        try_to_timestamp(pickup_datetime, 'yyyy-MM-dd'),
        try_to_timestamp(pickup_datetime, 'dd/MM/yyyy'),
        try_to_timestamp(pickup_datetime, 'dd-MMM-yyyy'),
        try_to_timestamp(pickup_datetime, 'dd.MM.yyyy')
    ) AS pickup_datetime_clean,
    COALESCE( 
        try_to_timestamp(dropoff_datetime, 'MM-dd-yyyy'),
        try_to_timestamp(dropoff_datetime, 'yyyy/MM/dd HH:mm'),
        try_to_timestamp(dropoff_datetime, 'MMMM dd, yyyy'),
        try_to_timestamp(dropoff_datetime, 'MM/dd/yyyy'),
        try_to_timestamp(dropoff_datetime, 'yyyy-MM-dd'),
        try_to_timestamp(dropoff_datetime, 'dd/MM/yyyy'),
        try_to_timestamp(dropoff_datetime, 'dd-MMM-yyyy'),
        try_to_timestamp(dropoff_datetime, 'dd.MM.yyyy') 
    ) AS dropoff_datetime_clean
    FROM gold_temp
""")

# Xóa các cột cũ chưa chuẩn hóa và thay bằng các cột sạch
gold_df = (
    gold_df
    .drop("pickup_datetime", "dropoff_datetime")
    .withColumnRenamed("pickup_datetime_clean", "pickup_datetime")
    .withColumnRenamed("dropoff_datetime_clean", "dropoff_datetime")
)

# Chuẩn hóa về chuỗi theo dạng thống nhất 'yyyy-MM-dd HH:mm'
gold_df = (
    gold_df
    .withColumn("pickup_datetime", date_format(col("pickup_datetime"), "yyyy-MM-dd HH:mm"))
    .withColumn("dropoff_datetime", date_format(col("dropoff_datetime"), "yyyy-MM-dd HH:mm"))
)

# Trích xuất khung giờ đón khách (pickup_hour)
gold_df = gold_df.withColumn("pickup_hour", hour(col("pickup_datetime")))

# Điền giá trị mặc định an toàn cho các trường hợp không phân tích được giờ
gold_df = gold_df.fillna({
    "pickup_hour": -1,
    "pickup_datetime": "0000-00-00 00:00",
    "dropoff_datetime": "0000-00-00 00:00"
})

# COMMAND ----------

# ==============================================================================
# 3. XÂY DỰNG CÁC BẢNG DATA MART PHỤC VỤ DASHBOARD BÁO CÁO
# ==============================================================================

# Mart 1: Tổng doanh thu tạo ra theo từng tài xế
driver_revenue_summary = (
    gold_df.groupBy("driver_id", "driver_name")
    .agg(round(sum("total_fare"), 2).alias("total_revenue"))
    .orderBy("total_revenue", ascending=False)
)

# Mart 2: Điểm đánh giá trung bình của từng tài xế
driver_rating_summary = (
    gold_df.groupBy("driver_id", "driver_name")
    .agg(round(avg("driver_rating"), 2).alias("avg_rating"))
    .orderBy("avg_rating", ascending=False)
)

# Mart 3: Tổng doanh thu phân bổ theo loại phương tiện (Sedan, SUV, Bike,...)
vehicle_revenue_summary = (
    gold_df.groupBy("vehicle_type")
    .agg(round(sum("total_fare"), 2).alias("sum_veh_type"))
    .orderBy("sum_veh_type", ascending=False)
)

# Mart 4: Tổng số chuyến đi của từng khách hàng (Xác định khách hàng thân thiết)
rider_trip_summary = (
    gold_df.groupBy("rider_id", "rider_name")
    .agg(count("trip_id").alias("total_rides"))
    .orderBy("total_rides", ascending=False)
)

# Mart 5: Doanh thu theo từng phương thức thanh toán (Tiền mặt, Thẻ tín dụng, Ví điện tử)
payment_revenue_summary = (
    gold_df.groupBy("payment_type")
    .agg(round(sum("total_fare"), 2).alias("rev_pay_type"))
    .orderBy("rev_pay_type", ascending=False)
)

# Mart 6: Phân tích các khung giờ cao điểm (Peak Hours)
peak_hours_summary = (
    gold_df.filter(col("pickup_hour") != -1)
    .groupBy("pickup_hour")
    .agg(count("trip_id").alias("peak_hour_rides"))
    .orderBy("peak_hour_rides", ascending=False)
)

# Mart 7: Thống kê số lượng theo nguyên nhân hủy chuyến
cancellation_summary = (
    gold_df.groupBy("cancellation_reason")
    .agg(count("trip_id").alias("cancelled_rides"))
    .orderBy("cancelled_rides", ascending=False)
)

# Mart 8: Top các địa điểm đón khách phổ biến nhất
top_pickup_summary = (
    gold_df.groupBy("pickup_location")
    .agg(count("trip_id").alias("visited_places"))
    .orderBy("visited_places", ascending=False)
)

# Mart 9: Top các địa điểm trả khách phổ biến nhất
top_dropoff_summary = (
    gold_df.groupBy("dropoff_location")
    .agg(count("trip_id").alias("dropoff_places"))
    .orderBy("dropoff_places", ascending=False)
)

# Mart 10: Phân bổ lượng người dùng theo hệ điều hành di động (iOS / Android)
platform_summary = (
    gold_df.groupBy("os_platform")
    .agg(count("trip_id").alias("os_count"))
    .orderBy("os_count", ascending=False)
)

# Mart 11: Mật độ chuyến đi theo từng vùng tăng giá (Surge Pricing Zones)
surge_zone_summary = (
    gold_df.groupBy("surge_zone")
    .agg(count("trip_id").alias("surge_zone_count"))
    .orderBy("surge_zone_count", ascending=False)
)

# COMMAND ----------

# ==============================================================================
# 4. GHI TẤT CẢ CÁC BẢNG GOLD VÀO DELTA LAKE
# ==============================================================================

# Bảng Fact tổng hợp chính
gold_df.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.gold_ridesharing")

# Các bảng Mart phân tích chuyên biệt
driver_revenue_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.driver_revenue_summary")
rider_trip_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.rider_trip_summary")
driver_rating_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.driver_rating_summary")
vehicle_revenue_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.vehicle_revenue_summary")
payment_revenue_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.payment_revenue_summary")
peak_hours_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.peak_hours_summary")
cancellation_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.cancellation_summary")
top_pickup_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.top_pickup_summary")
top_dropoff_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.top_dropoff_summary")
platform_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.platform_summary")
surge_zone_summary.write.format("delta").mode("overwrite").saveAsTable("ride_catalogue.default.surge_zone_summary")

print("-> Đã xuất bản thành công 11 bảng Data Mart tầng Gold vào Delta Lake!")
