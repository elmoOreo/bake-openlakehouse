import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# Connect to Trino/Presto engine
engine = create_engine(
    "trino://localhost:8080/iceberg/knowledge_graph",
    poolclass=NullPool,
    connect_args={
        "user": "admin",
        "http_headers": {
            "X-Presto-User": "admin"
        }
    }
)

# Explicitly use fully qualified identifier: catalog.schema.table
query = "SELECT subject, predicate, value FROM iceberg.knowledge_graph.global_triplestore LIMIT 25"

df = pd.read_sql(query, engine)

print("=== KNOWLEDGE GRAPH TRIPLESTORE SAMPLE ===")
print(df.to_string(index=False))