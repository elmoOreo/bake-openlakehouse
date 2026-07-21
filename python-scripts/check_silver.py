# check_postgres.py
import json
from sqlalchemy import create_engine, text

# Connect directly to the Silver stage database
engine = create_engine("postgresql://user:password@localhost:5432/document_lake")

with engine.connect() as conn:
    print("=== Silver Layer Staging Records Summary ===")
    
    # 1. Fetch metadata summary from staging_reports
    summary_query = text("""
        SELECT report_id, patient_id, extracted_at, s3_permanent_uri 
        FROM public.staging_reports;
    """)
    records = conn.execute(summary_query).fetchall()
    
    for row in records:
        print(f"Report ID : {row.report_id}")
        print(f"Patient ID: {row.patient_id}")
        print(f"Extracted : {row.extracted_at}")
        print(f"S3 URI    : {row.s3_permanent_uri}")
        print("-" * 50)

    print("\n=== Sample Extracted JSON Payload (Silver Layer) ===")
    
    # 2. Fetch and format the JSONB payload
    json_query = text("""
        SELECT raw_hierarchical_json 
        FROM public.staging_reports 
        LIMIT 1;
    """)
    raw_json = conn.execute(json_query).scalar()
    
    if raw_json:
        # Format JSON for clean terminal readability
        print(json.dumps(raw_json, indent=2))
    else:
        print("No staging records found in public.staging_reports.")