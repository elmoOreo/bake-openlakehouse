import boto3
from botocore.exceptions import ClientError

def inspect_entire_bronze_layer(bucket_name: str = "raw-lab-reports"):
    """
    Lists all objects in the Bronze MinIO bucket and displays 
    system and custom metadata for each file automatically.
    """
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )

    print("\n" + "=" * 60)
    print(f"📦 BRONZE LAYER INSPECTOR | Bucket: '{bucket_name}'")
    print("=" * 60)

    try:
        # 1. List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        if 'Contents' not in response or not response['Contents']:
            print(f"⚠️  Bucket '{bucket_name}' is currently empty!")
            print("   Upload PDF files to Bronze layer first.")
            print("=" * 60 + "\n")
            return

        object_count = len(response['Contents'])
        print(f"Found {object_count} file(s) in Bronze storage.\n")

        # 2. Iterate through every object and fetch its metadata
        for idx, item in enumerate(response['Contents'], start=1):
            key = item['Key']
            
            # Fetch full head object metadata
            meta_resp = s3_client.head_object(Bucket=bucket_name, Key=key)
            
            # Pre-sanitize variables to avoid backslashes inside f-strings
            etag_clean = str(meta_resp.get('ETag', 'N/A')).replace('"', '')
            
            print(f"[{idx}/{object_count}] 📄 File: {key}")
            print("-" * 50)
            print("  📊 SYSTEM METADATA:")
            print(f"     • File Size     : {meta_resp.get('ContentLength', 0):,} bytes")
            print(f"     • Content Type  : {meta_resp.get('ContentType', 'N/A')}")
            print(f"     • Uploaded At   : {meta_resp.get('LastModified')}")
            print(f"     • ETag Checksum : {etag_clean}")
            
            custom_metadata = meta_resp.get('Metadata', {})
            print("  🏷️  CUSTOM USER METADATA:")
            if custom_metadata:
                for k, v in custom_metadata.items():
                    print(f"     • {k}: {v}")
            else:
                print("     (None attached)")
            print("-" * 50 + "\n")

        print("=" * 60)
        print("✅ Bronze Layer Inspection Complete.")
        print("=" * 60 + "\n")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"❌ Error: Bucket '{bucket_name}' does not exist.")
        else:
            print(f"❌ MinIO S3 Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")

if __name__ == "__main__":
    inspect_entire_bronze_layer()