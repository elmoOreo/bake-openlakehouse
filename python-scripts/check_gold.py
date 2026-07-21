import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

engine = create_engine(
    "trino://localhost:8080/iceberg/clinical_analytics",
    poolclass=NullPool,
    connect_args={"user": "admin", "http_headers": {"X-Presto-User": "admin"}}
)

print("=== GOLD LAYER: PATIENTS MASTER ===")
df_patients = pd.read_sql("SELECT * FROM iceberg.clinical_analytics.patients", engine)
print(df_patients.to_string(index=False))

print("\n" + "="*60 + "\n")

print("=== GOLD LAYER: LAB OBSERVATIONS SAMPLE ===")
query_obs = """
SELECT 
    report_id, 
    test_name, 
    loinc_code, 
    numeric_value, 
    units, 
    interpretation 
FROM iceberg.clinical_analytics.lab_observations 
LIMIT 15
"""
df_obs = pd.read_sql(query_obs, engine)
print(df_obs.to_string(index=False))