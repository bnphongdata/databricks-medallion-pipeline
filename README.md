# databricks-medallion-pipeline

## Pipeline Dữ Liệu Gọi Xe Theo Kiến Trúc Medallion Trên Databricks (Lakehouse Pipeline)

Dự án Data Engineering xây dựng quy trình xử lý dữ liệu lớn (Big Data Processing) hoàn chỉnh từ đầu đến cuối trên nền tảng **Databricks**, áp dụng kiến trúc **Medallion Architecture (Bronze → Silver → Validation → Gold)** với định dạng lưu trữ **Delta Lake**, điều phối tự động bằng **Apache Airflow**, và trực quan hóa toàn diện bằng **Power BI Dashboard**.

---

## 1. Kiến Trúc Hệ Thống (Architecture)

Pipeline tổ chức dữ liệu theo luồng 4 tầng khép kín:

```
  [Raw CSV Data]
         │
         ▼  (PySpark: Raw Ingestion & Schema Inference)
  ┌─────────────────────────┐
  │      BRONZE LAYER       │  (Bảng Delta thô: bronze_ridesharing)
  └─────────────────────────┘
         │
         ▼  (PySpark: Cleansing, Deduplication, Imputation)
  ┌─────────────────────────┐
  │      SILVER LAYER       │  (Bảng Delta sạch: silver_ridesharing)
  └─────────────────────────┘
         │
         ▼  (PySpark Assertions: 8 Data Quality Gates / Circuit Breaker)
  ┌─────────────────────────┐
  │    VALIDATION LAYER     │  (Ngắt pipeline lập tức nếu vi phạm chất lượng)
  └─────────────────────────┘
         │
         ▼  (PySpark SQL: Multi-format Timestamps & 11 Data Marts)
  ┌─────────────────────────┐
  │       GOLD LAYER        │  (Bảng Fact & 11 Data Marts chuyên sâu)
  └─────────────────────────┘
         │
         ▼  (DirectQuery / Import)
  ┌─────────────────────────┐
  │    POWER BI DASHBOARD   │  (Báo cáo điều hành & phân tích kinh doanh 4 trang)
  └─────────────────────────┘
```

---

## 2. Công Nghệ Sử Dụng (Tech Stack)

| Thành phần | Công nghệ | Vai trò trong hệ thống |
| :--- | :--- | :--- |
| **Nền tảng Lakehouse** | [Databricks](https://www.databricks.com/) | Môi trường tính toán phân tán Spark và quản lý Unity Catalog |
| **Xử lý dữ liệu lớn** | [Apache PySpark](https://spark.apache.org/) | Xử lý DataFrame song song ở quy mô lớn |
| **Định dạng bảng** | [Delta Lake](https://delta.io/) | Đảm bảo tính toàn vẹn giao dịch ACID, Time Travel và Schema Enforcement |
| **Kiểm soát chất lượng** | PySpark Data Quality Gates | 8 bài kiểm tra ngắt mạch (Circuit Breaker) bảo vệ tầng Gold |
| **Điều phối Workflow** | [Apache Airflow](https://airflow.apache.org/) | Lập lịch, theo dõi trạng thái task và quản lý luồng phụ thuộc |
| **Phân tích & Trực quan** | Microsoft Power BI | Xây dựng Dashboard điều hành 4 trang chuyên sâu (`.pbix`) |

---

## 3. Cấu Trúc Thư Mục Dự Án

```text
databricks-medallion-pipeline/
├── data/
│   └── ridesharing_raw.csv        # Tệp dữ liệu mẫu đầy đủ 33 cột thuộc tính
├── notebooks/
│   ├── Bronze_layer.py            # Notebook nạp dữ liệu thô vào bảng Delta Bronze
│   ├── Silver_layer.py            # Notebook làm sạch, khử trùng lặp và điền khuyết
│   ├── Validation.py              # Notebook cổng kiểm soát chất lượng dữ liệu (Data Quality Gates)
│   └── Gold_layer.py              # Notebook tổng hợp chỉ số kinh doanh và xuất bản 11 Data Marts
├── screenshots/                   # Ảnh chụp thực tế 4 trang Power BI Dashboard
│   ├── Page -1.png                # Trang 1: Admin Overview & KPI Cards
│   ├── Page - 2 .png              # Trang 2: Revenue Analysis
│   ├── Page - 3.png               # Trang 3: Ride & Location Analysis
│   └── Page - 4.png               # Trang 4: Cancellation & Surge Analysis
├── Ridesharing_dashboard.pbix     # Tệp thiết kế Power BI Dashboard gốc
├── rides_pipeline.py              # Airflow DAG điều phối toàn bộ workflow
├── LICENSE                        # Giấy phép mã nguồn mở MIT
├── .gitignore                     # Danh sách loại trừ tệp rác
└── README.md                      # Tài liệu kỹ thuật chi tiết
```

---

## 4. Chi Tiết Quy Trình Xử Lý Các Tầng (Medallion Flow)

### 🥉 1. Tầng Bronze (Raw Ingestion)
* **Mục tiêu**: Nạp dữ liệu thô từ file CSV (`data/ridesharing_raw.csv` hoặc Unity Catalog Volumes) vào bảng Delta `ride_catalogue.default.bronze_ridesharing`.
* **Đặc tính**: Lưu giữ nguyên vẹn cấu trúc và bản ghi gốc, phục vụ mục đích kiểm toán (Auditability) và tái tạo dữ liệu khi cần.

### 🥈 2. Tầng Silver (Cleansing & Standardization)
* **Khử trùng lặp (Deduplication)**: Loại bỏ các bản ghi trùng lặp dựa trên khóa chính `trip_id`.
* **Lọc bỏ bản ghi lỗi**: Xóa các dòng thiếu khóa bắt buộc (`trip_id`, `rider_id`, `driver_id`).
* **Lọc nghiệp vụ hợp lệ**: Đảm bảo `total_fare > 0`, `distance_km > 0`, và `trip_duration_mins > 0`.
* **Điền khuyết thống kê (Imputation)**:
  * Điền giá trị trung bình (**Mean**) cho tọa độ địa lý (`pickup_lat`, `pickup_lng`, `dropoff_lat`, `dropoff_lng`) và làm tròn đến 6 chữ số thập phân.
  * Điền giá trị trung vị (**Median**) cho cước cơ bản (`base_fare`) để tránh tác động của giá trị ngoại lai (Outliers).
  * Điền giá trị trung bình (**Mean**) cho hệ số tăng giá (`surge_multiplier`) và làm tròn 1 chữ số thập phân.
* **Chuẩn hóa định dạng**: Chuyển đổi toàn bộ chuỗi phân loại (`vehicle_type`, `payment_type`, `ride_status`, `surge_zone`, `pickup_location`,...) về chữ thường (lowercase).
* **Lưu trữ**: Ghi vào bảng Delta `ride_catalogue.default.silver_ridesharing`.

### ✅ 3. Tầng Validation (Data Quality Gates / Circuit Breaker)
Trước khi dữ liệu được chuyển tiếp sang tầng Gold, dữ liệu Silver bắt buộc phải vượt qua **8 bài kiểm tra chất lượng dữ liệu nghiêm ngặt**:
1. **Row Count Check**: Xác nhận bảng Silver không được rỗng (`count > 0`).
2. **Duplicate Check**: Xác nhận 0 bản ghi trùng lặp theo `trip_id`.
3. **Non-negative Fares**: Không có bất kỳ bản ghi nào có `total_fare < 0`.
4. **Column Count Integrity**: Cấu trúc bảng phải có chính xác 33 cột thuộc tính.
5. **Schema Type Enforcement**: Kiểm tra tính chính xác kiểu dữ liệu của 16 trường số và chuỗi cốt lõi.
6. **Critical Null Check**: Đảm bảo 0 giá trị null trong các cột định danh khóa (`trip_id`, `rider_id`, `driver_id`).
7. **Rating Range Check**: Điểm đánh giá tài xế và hành khách phải nằm trong khoảng hợp lệ từ 1.0 đến 5.0 sao.
8. **Positive Metrics Check**: Quãng đường (`distance_km`) và thời lượng (`trip_duration_mins`) bắt buộc phải dương (> 0).

> **Cơ chế ngắt mạch (Circuit Breaker)**: Nếu bất kỳ bài kiểm tra nào thất bại, PySpark sẽ ném ra ngoại lệ `AssertionError` lập tức dừng pipeline, ngăn chặn triệt để nguy cơ xuất hiện số liệu sai lệch trên báo cáo kinh doanh.

### 🥇 4. Tầng Gold (Analytics & Business Data Marts)
* **Xử lý ngày giờ đa định dạng (Multi-format Timestamp Resolution)**: Sử dụng hàm `COALESCE` kết hợp `try_to_timestamp` xử lý an toàn 8 biến thể định dạng thời gian khác nhau của `pickup_datetime` và `dropoff_datetime`, chuẩn hóa về dạng đồng nhất `yyyy-MM-dd HH:mm`.
* **Xuất bản 11 bảng Data Marts chuyên sâu**:
  1. `gold_ridesharing`: Bảng Fact chi tiết chuyến đi.
  2. `driver_revenue_summary`: Tổng hợp doanh thu theo từng tài xế.
  3. `driver_rating_summary`: Đánh giá trung bình theo tài xế.
  4. `vehicle_revenue_summary`: Doanh thu phân bổ theo loại xe (Sedan, SUV, Luxury,...).
  5. `rider_trip_summary`: Tần suất và số lượng chuyến đi của từng khách hàng.
  6. `payment_revenue_summary`: Doanh thu theo phương thức thanh toán.
  7. `peak_hours_summary`: Phân tích lượng đặt xe theo các khung giờ trong ngày.
  8. `cancellation_summary`: Thống kê nguyên nhân hủy chuyến.
  9. `top_pickup_summary`: Các địa điểm đón khách có nhu cầu cao nhất.
  10. `top_dropoff_summary`: Các điểm đến phổ biến nhất.
  11. `platform_summary`: Phân bổ người dùng theo hệ điều hành di động (iOS / Android).
  12. `surge_zone_summary`: Mật độ nhu cầu tại các khu vực áp dụng hệ số tăng giá.

---

## 5. Báo Cáo Trực Quan Power BI (Dashboard Overview)

File báo cáo [**`Ridesharing_dashboard.pbix`**](file:///D:/databricks-medallion-pipeline/Ridesharing_dashboard.pbix) kết nối trực tiếp với các bảng Gold, bao gồm 4 trang chuyên biệt:

1. **Trang 1: Admin Dashboard (Tổng quan điều hành)**:
   * Thẻ KPI: Tổng doanh thu, tổng số chuyến đi, điểm đánh giá trung bình toàn hệ thống.
   * Doanh thu theo loại phương tiện và phương thức thanh toán.
2. **Trang 2: Revenue Analysis (Phân tích doanh thu)**:
   * Top 10 tài xế có doanh thu cao nhất.
   * Doanh thu theo từng vùng tăng giá (Surge Zones) và biến động cước phí.
3. **Trang 3: Ride & Location Analysis (Hành vi & Điểm đón trả)**:
   * Biểu đồ phân bổ chuyến đi theo 24 khung giờ (xác định giờ cao điểm sáng/tối).
   * Bản đồ nhiệt các khu vực đón/trả khách tập trung đông nhất.
4. **Trang 4: Cancellation & User Platform (Phân tích hủy chuyến & nền tảng)**:
   * Tỷ lệ hủy chuyến phân loại theo nguyên nhân (Tài xế trễ hẹn, Khách hủy, Tắc đường).
   * Tỷ lệ thị phần người dùng giữa iOS và Android.

---

## 6. Hướng Dẫn Vận Hành & Khởi Chạy

### Cách 1: Chạy trực tiếp trên Databricks
1. Đăng nhập vào workspace **Databricks** (hỗ trợ cả Databricks Community Edition miễn phí).
2. Tải tệp `data/ridesharing_raw.csv` lên **Unity Catalog Volume** hoặc **DBFS**.
3. Import 4 tệp notebook trong thư mục `notebooks/` vào workspace Databricks:
   * Chạy tuần tự: `Bronze_layer.py` → `Silver_layer.py` → `Validation.py` → `Gold_layer.py`.
4. Kết nối Power BI Desktop với Databricks SQL Warehouse để tải dữ liệu vào dashboard.

### Cách 2: Điều phối tự động bằng Apache Airflow
1. Đặt tệp `rides_pipeline.py` vào thư mục `dags/` của hệ thống Apache Airflow.
2. Cấu hình Databricks Connection trên Airflow (nếu kích hoạt trực tiếp Databricks Notebook Task).
3. Truy cập Airflow Web UI, kích hoạt DAG `ridesharing_medallion_pipeline` để tự động hóa toàn bộ chu trình xử lý định kỳ hàng ngày.

---

## 7. Thông Tin Tác Giả & Bản Quyền

* **Tác giả**: **Bùi Nhất Phong** ([github.com/bnphongdata](https://github.com/bnphongdata))
* **Email**: bnphong.data@gmail.com
* **Giấy phép**: Phát hành theo chuẩn giấy phép mã nguồn mở [MIT License](LICENSE).
