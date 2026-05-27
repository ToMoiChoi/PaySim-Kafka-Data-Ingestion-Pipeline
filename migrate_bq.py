import os
import sys
from dotenv import load_dotenv
from google.cloud import bigquery

# Sửa lỗi in tiếng Việt trên console Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# 1. Tải cấu hình từ file .env
load_dotenv(".env")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "ecommerce-db2025")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Set biến môi trường cho Google Auth
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(GOOGLE_APPLICATION_CREDENTIALS)

try:
    client = bigquery.Client(project=BQ_PROJECT_ID)
except Exception as e:
    print(f"Lỗi khởi tạo BigQuery Client: {e}")
    print("Vui lòng kiểm tra lại file JSON chứa key service account.")
    exit(1)

# 2. Cấu hình bảng nguồn và bảng đích
source_table = f"{BQ_PROJECT_ID}.paysim_dw.fact_binance_trades_v02"
dest_table = f"{BQ_PROJECT_ID}.paysim_dw.fact_binance_trades"

print(f"Đang chuẩn bị copy dữ liệu:")
print(f"  Từ: {source_table}")
print(f"  Sang: {dest_table}\n")

# 3. Cấu hình Job chạy qua API (Lách luật chặn DML của Free Tier)
# Tính năng này được BigQuery cho phép dùng miễn phí
job_config = bigquery.QueryJobConfig(
    destination=dest_table,
    write_disposition="WRITE_APPEND", # Chế độ: Thêm vào cuối bảng, không ghi đè
)

# Chỉ dùng SELECT, BigQuery API sẽ tự động đẩy kết quả SELECT vào bảng đích
query = f"""
    SELECT *
    FROM `{source_table}`
"""

print("Đang gửi yêu cầu (Query Job) lên BigQuery...")
try:
    query_job = client.query(query, job_config=job_config)
    
    # Đợi tiến trình hoàn tất
    query_job.result()
    print("✅ COPY DỮ LIỆU THÀNH CÔNG!")
    print(f"Toàn bộ dữ liệu đã được đẩy vào {dest_table}.")
except Exception as e:
    print(f"❌ Có lỗi xảy ra trong quá trình copy: {e}")
