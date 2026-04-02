# Hướng dẫn thiết kế Power BI Dashboard cho Khóa luận (Real-time Streaming)

Vì cốt lõi khóa luận của bạn là **Data Streaming Real-time**, Power BI cần được tinh chỉnh đặc biệt để thể hiện được sức mạnh của luồng dữ liệu liên tục từ Binance và Kafka. 

Tuy nhiên do Power BI mặc định tải dữ liệu kiểu tĩnh (Import), bạn phải cấu hình theo **Mô hình Hỗn hợp (Composite Model)** và bật **Làm mới trang tự động (Auto Page Refresh)**. Dưới đây là "bí kíp" từ A-Z để bạn tạo một Dashboard hoàn hảo:

---

## 1. Kết nối và Thiết lập Mô hình dữ liệu (Data Modeling)

### Kéo dữ liệu từ PostgreSQL
1. Mở Power BI Desktop -> Menu **Get Data** -> Trỏ vào **PostgreSQL database**.
2. Nhập thông số: `Server: localhost`, `Database: paysim_dw`. Chuyển sang Data Connectivity mode: **DirectQuery** (Tuyệt đối quan trọng để lấy Real-time).
3. Đăng nhập với User `paysim` / Password `paysim123`.
4. Chọn và tích vào toàn bộ các bảng: `fact_transactions`, `dim_users`, `dim_merchants`, `dim_transaction_type`, `dim_account`, `dim_location`, `dim_channel`, `dim_date`, `dim_time`.

### Chuyển đổi mô hình lưu trữ (Storage Mode Optimization)
Vì `fact_transactions` thay đổi liên tục, nó phải ở dạng **DirectQuery**. Nhưng các bảng Dimension thì không đổi (Ví dụ danh sách User), nên hãy đổi chúng sang **Import**.
- Trong thanh bên của PBI, mở View **Model** (icon hình lưới).
- Kéo thả tạo Relationship (Cáp nối 1-nhiều):
  - Kéo `type_id` từ `dim_transaction_type` sang `type_id` của `fact_transactions`.
  - Làm tương tự cho User, Merchant, Khoảng thời gian, Địa điểm v.v.
- **Tối ưu cực đại:** Click lần lượt vào từng bảng Dim (như `dim_users`), liếc sang thanh *Properties* bên phải -> ở chỗ *Advanced* -> Storage mode -> Đổi từ *DirectQuery* sang **Dual**.
*(Mô hình Dual Storage đỉnh cao này giúp biểu đồ nhảy theo mili-giây mà không làm cháy CPU Database).*

---

## 2. Viết Công thức DAX Lõi (Làm "linh hồn" cho Dashboard)

Bấm chuột phải vào bảng `fact_transactions`, chọn **New Measure** và dán lần lượt các công thức DAX "ăn điểm" sau:

```dax
-- 1. Tổng khối lượng giao dịch
Total Volume = SUM('fact_transactions'[amount])

-- 2. Tổng số giao dịch 
Total Transactions = COUNTROWS('fact_transactions')

-- 3. Tổng điểm thưởng phát ra
Total Rewards = SUM('fact_transactions'[reward_points])

-- 4. Tốc độ luồng xử lý (Trades per minute)
-- Tính tổng giao dịch trong 1 phút qua
TPM (Trades per Min) = 
CALCULATE(
    [Total Transactions], 
    'fact_transactions'[transaction_time] >= (NOW() - 1/1440)
)

-- 5. Lọc giao dịch Crypto (Từ Binance)
Crypto Volume = 
CALCULATE(
    [Total Volume],
    'dim_transaction_type'[type_category] = "Crypto Trading"
)

-- 6. Tỷ lệ gian lận hoặc Cảnh báo Cá mập
Fraud/Whale Events = SUM('fact_transactions'[isFraud]) + SUM('fact_transactions'[isFlaggedFraud])
```

---

## 3. Thiết kế Bố cục (Layout): 2 Dashboard chuyên biệt

Bạn nên tạo **2 Page** trên file Power BI để khẳng định tầm vóc của dự án KLTN. Khuyên dùng Nền màu tối (Dark Theme) để bật lên tính kỹ thuật.

### Bố cục 1: TECHNICAL MONITORING (Giám sát Real-time Pipeline)
Trang này dùng để show cho Giáo viên / Hội đồng chấm thấy khả năng xử lý Real-time Streaming của Kafka & Spark.

- **Bật hiệu ứng nhảy dữ liệu:** Ở khoảng trắng của trang Dashboard -> Nhấn cây lăn sơn (Format page) -> Tích bật **Page refresh** -> Đặt thông số tự đồng làm mới (Auto page refresh) là `1 Second` hoặc `3 Seconds` tuỳ cấu hình máy bạn (Chỉ hoạt động ở mode DirectQuery!).
- **Thẻ Card (Phía trên cùng):**
  - KPI 1: `Total Transactions`
  - KPI 2: `TPM (Trades per Min)`
  - KPI 3: Vừa viết xong `Total Volume`
- **Line Chart (Kéo dài giữa màn hình):** Biểu đồ thể hiện Tốc độ / Lưu lượng mạng. 
  - Trục X: `transaction_time` (Nhớ định dạng hiển thị tới Giây).
  - Trục Y: `Total Transactions` và `Total Volume`.
- **Donut Chart 1 (Trái Dưới):** Phân loại luồng dữ liệu 
  - Legend: `type_category` (Fiat Banking vs Crypto Trading).
  - Value: `Total Transactions`.
- **Donut Chart 2 (Phải Dưới):** Cơ cấu Maker-Taker của tiền số. Nhồi trường `is_buyer_maker` bạn vừa vất vả lập ra.

### Bố cục 2: BUSINESS & REWARDS INSIGHTS (Hiệu ứng nghiệp vụ)
Trang này show ý nghĩa nghiệp vụ (Domain): Việc xử lý Big Data mang lại giá trị kinh doanh gì?

- **Map (Biểu đồ bản đồ):** 
  - Location: Trường `city` (Từ bảng `dim_location`).
  - Bubble size (Kích thước bọt): `Total Rewards`. (Cho ban giám khảo thấy tiền thưởng đang phân phát nóng về tỉnh/thành nào).
- **Matrix (Bảng xếp hạng):** Tìm ra khách hàng VIP theo thời gian thực.
  - Rows: `user_id` / `user_segment`
  - Columns: Bỏ trống
  - Values: `Total Volume`, `Total Rewards`. Click mũi tên trên cột Total Rewards để Sort (Sắp xếp) giảm dần. Khách hàng giao dịch Crypto cực khủng trên Binance sẽ lập tức nhảy Top 1 và kiếm bộn điểm tự động.
- **Bar Chart (Biểu đồ ngang xếp hạng merchants):**
  - Trục Y: `merchant_name`
  - Trục X: `Total Volume`
- **Slicers (Bộ lọc bên trái):** Gắn bộ lọc `Channel Name` (Web, App) và bộ lọc `Date` để giảng viên thao tác trực tiếp.

> [!TIP]
> **Điểm nhấn ăn điểm KLTN:** Lúc trình bày, bạn hãy bật cả 2 màn hình. Bên trái là Terminal màu đen đang chạy `make run-spark` và `make run-live`. Bên phải bật Full-screen trang PowerBI (Page 1). Mọi người sẽ thấy cứ mỗi khi giao dịch Binance giật trên màn hình đen, thì các con số trên Line Chart và thẻ KPI ở bên phải đồng loạt "NHẢY MÚA" ngay lập tức, đây sẽ là cực khoái cảm của Streaming Architecture mà các dự án Batch Processing truyền thống không làm được!
