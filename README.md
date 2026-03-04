# PaySim Kafka Data Ingestion Pipeline

Dự án này thực hiện việc đọc dữ liệu mô phỏng giao dịch tài chính từ bộ dataset [PaySim1](https://www.kaggle.com/datasets/ealaxi/paysim1), xử lý biến đổi dữ liệu, bơm lỗi (fault injection), và đẩy thẳng các luồng sự kiện vào Apache Kafka.

## Kiến trúc & Luồng xử lý

1. **Đọc dữ liệu:** `kafka_producer.py` đọc file `PS_2017...log.csv`.
2. **Biến đổi dữ liệu:** Lọc ra các bản ghi có `type == "PAYMENT"`. Thêm trường `transaction_id` (UUID4) và `event_timestamp` (thời gian hiện tại chuẩn ISO-8601).
3. **Bơm lỗi (Fault Injection):** Sao chép ngẫu nhiên 10% số lượng records (với cùng `transaction_id` để mô phỏng dữ liệu bị lặp do lỗi mạng/duplicate event) và trộn lại vào luồng dữ liệu chính.
4. **Kafka Producer:** Bắn dữ liệu (dạng JSON) vào topic `payment_events` của Kafka chạy qua Docker Compose.

## Yêu cầu Hệ thống (Prerequisites)

- Python 3.8+
- Docker & Docker Compose
- Tài khoản Kaggle (để tải dataset nếu chưa có)

## Cấu trúc thư mục

```text
KLTN_2/
├── data/
│   └── PS_20174392719_1491204439457_log.csv  # File dataset (sau khi giải nén)
├── venv/                                     # Môi trường Python ảo (Virtual Environment)
├── .env                                      # File lưu cấu hình tham số môi trường
├── docker-compose.yml                        # Cấu hình Kafka & Zookeeper Docker
├── download_dataset.py                       # Script tải dataset từ Kaggle
├── kafka_producer.py                         # Script chính để chạy luồng ingestion
├── requirements.txt                          # Các thư viện Python cần thiết
└── README.md                                 # File hướng dẫn này
```

## Các Bước Cài đặt & Chạy (Quickstart)

### Bước 0: (Tùy chọn) Tải Data từ Kaggle
Nếu bạn chưa có file `PS_2017...log.csv`, bạn có thể tải tự động bằng `download_dataset.py`.
Yêu cầu:
1. Đăng nhập Kaggle -> Account -> Create New API Token (sẽ tải về file `kaggle.json`).
2. Đặt `kaggle.json` vào thư mục `~/.kaggle/` (với Windows là `C:\Users\<Tên_User>\.kaggle\kaggle.json`).
3. Chạy `python download_dataset.py` sau khi đã thiết lập môi trường Python (Bước 2).


### Bước 1: Khởi động Kafka & Zookeeper

Mở terminal ở thư mục dự án và chạy Docker Compose để khởi tạo Infrastructure:

```bash
docker-compose up -d
```

Để kiểm tra xem Kafka và Zookeeper đã chạy hay chưa:

```bash
docker ps
```
(Bạn sẽ thấy 2 container là `kafka` và `zookeeper` đang chạy)

### Bước 2: Thiết lập Môi trường Python

Tạo và kích hoạt môi trường ảo (virtual environment), sau đó cài đặt các gói phụ thuộc:

```powershell
# Tạo môi trường ảo
python -m venv venv

# Kích hoạt môi trường (Windows PowerShell)
.\venv\Scripts\activate

# Cài đặt thư viện
pip install -r requirements.txt
```

### Bước 3: Cấu hình biến môi trường (`.env`)

Mở file `.env` và tùy chỉnh theo ý muốn. File mẫu trông như thế này:

```env
# Mặc định cấu hình
CSV_PATH=data/PS_20174392719_1491204439457_log.csv
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=payment_events
DUPLICATE_RATIO=0.10
BATCH_SIZE=65536
LINGER_MS=50
BUFFER_MEMORY=67108864
```

### Bước 4: Chạy Kafka Producer

Khởi chạy script ingestion để bắt đầu quá trình đọc, xử lý và bắn dữ liệu vào Kafka:

```powershell
# Chắc chắn rằng venv đang được kích hoạt
python kafka_producer.py
```

Console sẽ log ra quá trình xử lý:
- Số lượng bản ghi `PAYMENT` được lọc ra.
- Quá trình bơm lỗi Duplicate (10%).
- Tiến độ bắn vào Kafka (tốc độ tham khảo khoảng 6000 - 8000 msg/s tùy cấu hình máy).
- Thống kê chốt (tổng số gửi thành công, số lượng duplicates...).

### Bước 5: Kiểm tra dữ liệu trong Kafka (Tuỳ chọn)

Để xác nhận dữ liệu đã được bắn vào topic `payment_events`, hãy đọc thử (consume) một vài message từ Kafka broker:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment_events --from-beginning --max-messages 5
```

Bạn sẽ thấy dữ liệu JSON in ra như sau:
```json
{"step": 1, "type": "PAYMENT", "amount": 9839.64, "nameOrig": "C1231006815", "oldbalanceOrg": 170136.0, "newbalanceOrig": 160296.36, "nameDest": "M1979787155", "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0, "isFlaggedFraud": 0, "transaction_id": "848bb28a-...", "event_timestamp": "2026-03-03T13:20:00+00:00"}
```

## Cách Tùy Biến (Customizing Pipelines)

Bạn có thể chỉnh sửa file `.env` để làm thay đổi logic hoạt động của script theo ý mình:
- Đổi file CSV đường dẫn: Sửa `CSV_PATH`.
- Đổi % lỗi lặp Duplicate: Cập nhật `DUPLICATE_RATIO`.
- Chỉnh hiệu suất thông lượng gửi Kafka: Điều chỉnh `BATCH_SIZE`, `LINGER_MS`, `BUFFER_MEMORY`.
