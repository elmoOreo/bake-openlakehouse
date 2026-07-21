# check_postgres.py
from sqlalchemy import create_engine, text

# Using the exact same connection base string your pipeline uses
engine = create_engine("postgresql://user:password@localhost:5432/metastore_db")

with engine.connect() as conn:
    # 1. Query the system catalog for all database names
    dbs = conn.execute(text("SELECT datname FROM pg_database;")).fetchall()
    print("=== Databases visible to your Python environment ===")
    for db in dbs:
        print(f"-> {db[0]}")