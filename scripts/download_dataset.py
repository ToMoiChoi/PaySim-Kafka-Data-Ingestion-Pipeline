import os
import zipfile
import logging
from kaggle.api.kaggle_api_extended import KaggleApi

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATASET_NAME = "ealaxi/paysim1"
DOWNLOAD_DIR = "./data"
EXPECTED_FILE_NAME = "PS_20174392719_1491204439457_log.csv"

def download_and_extract_dataset():
    # Make sure the data directory exists
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    expected_file_path = os.path.join(DOWNLOAD_DIR, EXPECTED_FILE_NAME)
    
    # Check if file already exists
    if os.path.exists(expected_file_path):
        logging.info(f"Dataset already exists at {expected_file_path}. No need to download.")
        return

    logging.info(f"Authenticating with Kaggle API...")
    try:
        api = KaggleApi()
        api.authenticate()
    except Exception as e:
        logging.error(f"Failed to authenticate with Kaggle. Ensure KAGGLE_USERNAME and KAGGLE_KEY are set or kaggle.json is in ~/.kaggle/")
        logging.error(f"Error details: {e}")
        return

    logging.info(f"Downloading dataset {DATASET_NAME} to {DOWNLOAD_DIR}...")
    try:
        api.dataset_download_files(DATASET_NAME, path=DOWNLOAD_DIR, unzip=True)
        logging.info("Download and extraction complete!")
        
        # Verify the file exists after extraction
        if os.path.exists(expected_file_path):
            logging.info(f"Successfully verified {EXPECTED_FILE_NAME} exists.")
        else:
            logging.warning(f"Downloaded dataset, but {EXPECTED_FILE_NAME} was not found. Please check manually.")
            
    except Exception as e:
        logging.error(f"Failed to download dataset. Error details: {e}")

if __name__ == "__main__":
    download_and_extract_dataset()
