# Hướng dẫn Chạy Toàn bộ Code (End-to-End Execution Guide)

Đây là các bước để chạy toàn bộ hệ thống xử lý dữ liệu giao dịch thời gian thực (PaySim Fintech Pipeline).

## 1. Chuẩn bị Môi trường (Prerequisites)

*   **Docker Desktop**: Đã được cài đặt và đang chạy.
*   **Python 3.10+**: Đã cài đặt `virtualenv`.
*   **Hadoop binaries (Windows only)**: Hệ thống yêu cầu `winutils.exe` để chạy Spark Streaming cục bộ. 
    *   *Tôi đã cài đặt sẵn vào thư mục `C:\hadoop\bin` cho bạn.*

## 2. Khởi chạy Hạ tầng (Infrastructure)

Mở Terminal và chạy lệnh sau để khởi động PostgreSQL và Kafka:

```powershell
docker-compose up -d
```

Đảm bảo các container `postgres`, `kafka`, và `zookeeper` đang ở trạng thái **Running**.

## 3. Khởi tạo Cơ sở Dữ liệu (Database Setup)

Chạy lệnh sau để tạo Schema (Star Schema) và nạp dữ liệu danh mục (Dimension) vào PostgreSQL:

```powershell
# 1. Tạo các bảng (Fact & Dimensions)
python warehouse/postgres_schema.py

# 2. Nạp dữ liệu giả lập (Users, Accounts, Time, ...)
python warehouse/seed_dimensions_pg.py
```

## 4. Chạy Pipeline Xử lý (Data Pipeline)

Bạn cần mở **2 Terminal riêng biệt**:

### Terminal 1: Chạy Spark Processor (Receiver)
Đây là "bộ não" thực hiện khử trùng lặp giao dịch và tính điểm thưởng.
```powershell
python processor/spark_processor.py
```
*Đợi cho đến khi bạn thấy thông báo: `[DONE] Stream đang chạy. Nhấn Ctrl+C để dừng.`*

### Terminal 2: Chạy Kafka Producer (Sender)
Đây là trình giả lập Core Banking đẩy dữ liệu giao dịch vào Kafka.
```powershell
python producer/kafka_producer.py
```

## 5. Kiểm tra Kết quả (Verification)

Sau khi Producer báo đã gửi dữ liệu, bạn có thể kiểm tra PostgreSQL để xem điểm thưởng đã được tính và lưu vào kho:

```powershell
docker exec -i postgres psql -U paysim -d paysim_dw -c "SELECT COUNT(*) FROM fact_transactions;"
```

Hoặc xem 5 giao dịch mới nhất với điểm thưởng:
```powershell
docker exec -i postgres psql -U paysim -d paysim_dw -c "SELECT transaction_id, account_id, amount, reward_points FROM fact_transactions LIMIT 5;"
```

---
**Lưu ý**: Nếu chạy trên Windows, Spark có thể yêu cầu nhiều RAM (đã cấu hình 3GB). Hãy đảm bảo máy tính còn trống khoảng 4-6GB RAM.
