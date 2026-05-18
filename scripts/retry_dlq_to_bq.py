import os
import glob
import shutil
import logging
from dotenv import load_dotenv
from google.cloud import bigquery

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RetryDLQ")

def main():
    # 1. Setup paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
    
    # Load .env
    load_dotenv(ENV_PATH)
    
    # 2. Get BigQuery configs
    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET    = os.getenv("BQ_DATASET", "paysim_dw")
    BQ_TABLE_FACT = "fact_binance_trades"
    GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    if not BQ_PROJECT_ID:
        logger.error("BQ_PROJECT_ID is not set in .env")
        return

    # Ensure GOOGLE_APPLICATION_CREDENTIALS is absolute
    if GOOGLE_CREDENTIALS and not os.path.isabs(GOOGLE_CREDENTIALS):
        GOOGLE_CREDENTIALS = os.path.join(PROJECT_ROOT, GOOGLE_CREDENTIALS)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS
        
    if not os.path.exists(GOOGLE_CREDENTIALS):
        logger.error(f"Credentials file not found at: {GOOGLE_CREDENTIALS}")
        return

    # 3. Setup DLQ Directories
    DLQ_DIR = os.getenv("BQ_DLQ_DIR", os.path.join(PROJECT_ROOT, "dlq_bq_failed"))
    SUCCESS_DIR = os.path.join(PROJECT_ROOT, "dlq_bq_success")
    os.makedirs(SUCCESS_DIR, exist_ok=True)

    if not os.path.exists(DLQ_DIR):
        logger.info(f"DLQ directory does not exist: {DLQ_DIR}. Nothing to retry.")
        return

    # Find all parquet files in the DLQ directory
    parquet_files = glob.glob(os.path.join(DLQ_DIR, "*.parquet"))
    
    if not parquet_files:
        logger.info("No failed parquet files found in DLQ directory. Everything is up to date!")
        return

    logger.info(f"Found {len(parquet_files)} failed batches to retry.")

    # 4. Initialize BigQuery Client
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.PARQUET,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    # 5. Process each file
    success_count = 0
    for file_path in parquet_files:
        filename = os.path.basename(file_path)
        logger.info(f"Uploading {filename} to BigQuery...")
        
        try:
            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                job.result()  # Wait for the job to complete
                
            logger.info(f"[SUCCESS] {filename} uploaded successfully.")
            
            # Move to success folder so we don't process it again
            dest_path = os.path.join(SUCCESS_DIR, filename)
            shutil.move(file_path, dest_path)
            success_count += 1
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to upload {filename}: {e}")

    logger.info("=" * 50)
    logger.info(f"DLQ Retry Summary: {success_count}/{len(parquet_files)} files successfully uploaded.")
    if success_count > 0:
        logger.info(f"Successfully uploaded files have been moved to: {SUCCESS_DIR}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
