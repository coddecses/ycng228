import os
import logging
import pandas as pd

logger = logging.getLogger()

def save_data_to_csv(data, output_dir, file_name):
    """
    Save the data to a CSV file locally.
    """
    file_path = os.path.join(output_dir, file_name)
    try:
        data.to_csv(file_path)
        logger.info(f"Data successfully saved locally at {file_path}")
    except Exception as e:
        logger.error(f"Error saving data locally to {file_path}: {e}")

def save_data_to_gcs(data, bucket_name, file_name):
    """
    Save the data to a Google Cloud Storage bucket.
    """
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    try:
        blob.upload_from_string(data.to_csv(), content_type='text/csv')
        logger.info(f"Data successfully uploaded to GCS bucket {bucket_name} with filename {file_name}")
    except Exception as e:
        logger.error(f"Error uploading data to GCS bucket {bucket_name}: {e}")
