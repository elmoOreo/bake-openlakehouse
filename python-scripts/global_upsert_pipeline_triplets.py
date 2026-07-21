import os
import io
import re
import boto3
import pdfplumber
import prestodb

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PRESTO_CONNECTION_OPTIONS = {
    "host": "localhost",
    "port": 8080,
    "user": "admin",
    "catalog": "iceberg"
}

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
    "raw_bucket": "raw-lab-reports"
}

TARGET_SCHEMA = "knowledge_graph"
TARGET_TABLE = f"{TARGET_SCHEMA}.global_triplestore"
DEBUG_MODE = False

# ==============================================================================
# UTILITIES & PARSER
# ==============================================================================
def clean_string(val):
    if not val:
        return ""
    cleaned = re.sub(r'[\"\s\n]+', ' ', val).strip()
    return cleaned.replace("'", "''")

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_CONFIG["endpoint_url"],
        aws_access_key_id=MINIO_CONFIG["aws_access_key_id"],
        aws_secret_access_key=MINIO_CONFIG["aws_secret_access_key"],
        config=boto3.session.Config(signature_version='s3v4')
    )

def _init_presto_schema(cursor):
    """Guarantees that the knowledge_graph schema and target triplestore table exist in Iceberg."""
    print(f"[+] Verifying existence of schema [{TARGET_SCHEMA}] and table [{TARGET_TABLE}]...")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            subject VARCHAR,
            predicate VARCHAR,
            value VARCHAR
        ) WITH (format = 'PARQUET')
    """)
    print("[+] Schema and triplestore table verified successfully.")

def extract_semantic_triples_from_s3(s3_client, bucket: str, s3_key: str):
    """Fetches PDF binary stream from MinIO S3 Bronze layer and parses triples directly in memory."""
    print(f"[+] Fetching Bronze raw binary stream from MinIO: s3://{bucket}/{s3_key}")
    
    s3_object = s3_client.get_object(Bucket=bucket, Key=s3_key)
    pdf_bytes = s3_object['Body'].read()

    triples = []
    
    # Process PDF from memory buffer
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        full_text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    
    if DEBUG_MODE:
        print("\n--- DEBUG: RAW TEXT EXTRACTED FROM MINIO PDF ---")
        print(full_text[:1500])
        print("------------------------------------------------\n")
    
    uhid_match = re.search(r"UHID/MR No\s*,?\s*:\s*([\w\.]+)", full_text)
    if not uhid_match:
        print("[-] Warning: Failed to extract UHID. Aborting parsing for this file.")
        return []
        
    uhid = clean_string(uhid_match.group(1))
    subject_patient = f"patient:{uhid}"
    
    name_match = re.search(r"Patient Name\s*,?\s*:\s*(Ms\.|Mr\.)?\s*([\w\s]+)", full_text)
    if name_match:
        patient_name = clean_string(name_match.group(2))
        prefix = name_match.group(1) if name_match.group(1) else ""
        triples.append((subject_patient, "has_name", f"{prefix} {patient_name}".strip()))
        
    age_match = re.search(r"Age/Gender\s*,?\s*:\s*([\d\s\w/]+)", full_text)
    if age_match:
        age_gender = clean_string(age_match.group(1))
        triples.append((subject_patient, "has_age_gender", age_gender))

    visit_match = re.search(r"Visit ID\s*,?\s*:\s*([\w\.]+)", full_text)
    visit_id = clean_string(visit_match.group(1)) if visit_match else "UNKNOWN_VISIT"
    subject_visit = f"visit:{visit_id}"
    
    triples.append((subject_patient, "has_visit", subject_visit))

    date_match = re.search(r"Collected\s*,?\s*:\s*([\w/:\s]+AM|PM)", full_text)
    if date_match:
        collected_date = clean_string(date_match.group(1))
        triples.append((subject_visit, "collected_at", collected_date))

    row_pattern = re.compile(
        r"(?P<test_name>^[A-Z\s\(\),\.\/]+?)\s+"
        r"(?P<result>[\d\.,]+)\s+"
        r"(?P<unit>[\w/%]+)\s+"
        r"(?P<ref_interval>[\d\.\-]+)",
        re.MULTILINE
    )
    
    for match in row_pattern.finditer(full_text):
        test_name = clean_string(match.group("test_name"))
        result_value = clean_string(match.group("result"))
        unit = clean_string(match.group("unit"))
        ref_interval = clean_string(match.group("ref_interval"))
        
        if test_name in ["UHID/MR No", "Visit ID", "Age/Gender", "Patient Name", "Collected", "Received", "Reported"]:
            continue
            
        normalized_test = test_name.lower().replace(" ", "_").replace(",", "")
        subject_obs = f"obs:{visit_id}_{normalized_test}"
        
        triples.append((subject_visit, "has_observation", subject_obs))
        triples.append((subject_obs, "observation_type", test_name))
        triples.append((subject_obs, "has_numeric_value", result_value))
        triples.append((subject_obs, "has_unit", unit))
        triples.append((subject_obs, "has_reference_interval", ref_interval))
        
    return triples

# ==============================================================================
# PIPELINE DATA EXECUTION STAGES
# ==============================================================================
def execute_presto_pipeline(triples):
    conn = prestodb.dbapi.connect(**PRESTO_CONNECTION_OPTIONS)
    cursor = conn.cursor()
    
    try:
        # Step 0: Ensure target schema & table exist
        _init_presto_schema(cursor)

        # Step A: Drop existing staging table if present
        cursor.execute(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.pipeline_direct_staging")
        
        # Step B: Create temporary staging table layout
        cursor.execute(f"""
            CREATE TABLE {TARGET_SCHEMA}.pipeline_direct_staging (
                subject VARCHAR,
                predicate VARCHAR,
                value VARCHAR
            ) WITH (format = 'PARQUET')
        """)
        
        print("[+] Packaging extracted triples into an ANSI SQL batch block...")
        value_strings = [f"('{t[0]}', '{t[1]}', '{t[2]}')" for t in triples]
        all_values_sql = ",\n".join(value_strings)
        
        insert_query = f"INSERT INTO {TARGET_SCHEMA}.pipeline_direct_staging (subject, predicate, value) VALUES \n{all_values_sql}"
        cursor.execute(insert_query)
        
        # Step D: Check target table record count and merge/upsert
        cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
        target_count = cursor.fetchone()[0]
        
        if target_count == 0:
            print(f"[+] Target table {TARGET_TABLE} is empty. Initializing baseline snapshot data stream...")
            cursor.execute(f"INSERT INTO {TARGET_TABLE} SELECT * FROM {TARGET_SCHEMA}.pipeline_direct_staging")
        else:
            print(f"[+] Compiling merge engine constraints for {TARGET_TABLE}...")
            cursor.execute(f"""
                MERGE INTO {TARGET_TABLE} target
                USING {TARGET_SCHEMA}.pipeline_direct_staging source
                ON target.subject = source.subject 
                   AND target.predicate = source.predicate
                WHEN MATCHED THEN
                   UPDATE SET value = source.value
                WHEN NOT MATCHED THEN
                   INSERT (subject, predicate, value) 
                   VALUES (source.subject, source.predicate, source.value)
            """)
            
        cursor.execute(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.pipeline_direct_staging")
        print("[+] Upsert transaction successfully committed to Iceberg.")
        
    except Exception as e:
        print(f"[-] Critical: Transaction aborted due to engine fault: {str(e)}")
        raise e
    finally:
        cursor.close()
        conn.close()

# ==============================================================================
# RUNTIME ENTRYPOINT
# ==============================================================================
if __name__ == "__main__":
    s3 = get_s3_client()
    raw_bucket = MINIO_CONFIG["raw_bucket"]
    
    print(f"[Stage 1] Scanning MinIO Bronze Bucket [{raw_bucket}] for raw PDF objects...")
    
    response = s3.list_objects_v2(Bucket=raw_bucket)
    pdf_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.pdf')]
    
    if not pdf_keys:
        print(f"[-] No raw PDF files found in MinIO bucket [{raw_bucket}]. Run Stage 1 upload first.")
    else:
        for s3_key in pdf_keys:
            print(f"\n[+] Processing raw asset: {s3_key}")
            extracted_triples = extract_semantic_triples_from_s3(s3, raw_bucket, s3_key)
            print(f"[+] Extracted {len(extracted_triples)} semantic statements.")
            
            if extracted_triples:
                print("[Stage 2] Triggering Unified Presto Knowledge Graph Ingestion...")
                execute_presto_pipeline(extracted_triples)
                print("[+] Pipeline Execution Finalized Successfully!")