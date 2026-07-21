import os
import io
import logging
import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def upload_raw_reports():
    minio_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        config=boto3.session.Config(signature_version='s3v4')
    )
    
    raw_bucket = "raw-lab-reports"
    
    # Ensure bucket exists
    try:
        minio_client.head_bucket(Bucket=raw_bucket)
    except Exception:
        logging.info(f"Creating missing MinIO bucket [{raw_bucket}]...")
        minio_client.create_bucket(Bucket=raw_bucket)

    # Locate historical_reports
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    input_directory = os.path.join(project_root, "historical_reports")
    
    pdf_files = [f for f in os.listdir(input_directory) if f.endswith('.pdf')]
    if not pdf_files:
        logging.warning("No PDF reports found in historical_reports folder.")
        return

    for filename in pdf_files:
        local_path = os.path.join(input_directory, filename)
        year_prefix = filename[0:4] if filename[0:4].isdigit() else "2026"
        month_prefix = filename[4:6] if filename[4:6].isdigit() else "05"
        s3_key = f"reports/{year_prefix}/{month_prefix}/{filename}"
        
        with open(local_path, 'rb') as f:
            minio_client.upload_fileobj(io.BytesIO(f.read()), raw_bucket, s3_key)
            
        logging.info(f"[STAGE 1 UPLOAD SUCCESS] {filename} -> s3://{raw_bucket}/{s3_key}")

if __name__ == "__main__":
    upload_raw_reports()