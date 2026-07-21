import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from pyvis.network import Network

# 1. Connect to Presto Knowledge Graph Schema
engine = create_engine(
    "trino://localhost:8080/iceberg/knowledge_graph",
    poolclass=NullPool,
    connect_args={"user": "admin", "http_headers": {"X-Presto-User": "admin"}}
)

# 2. Fetch Triples from Iceberg Catalog
query = "SELECT subject, predicate, value FROM iceberg.knowledge_graph.global_triplestore LIMIT 100"
df = pd.read_sql(query, engine)

# 3. Build Interactive Network Graph
net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", directed=True)

# Add Nodes and Directed Edges
for _, row in df.iterrows():
    sub, pred, val = row["subject"], row["predicate"], row["value"]
    
    # Color-code node types visually
    sub_color = "#97C2FC" if "patient:" in sub else ("#FFFF00" if "visit:" in sub else "#FB7E81")
    val_color = "#97C2FC" if "patient:" in val else ("#FFFF00" if "visit:" in val else "#2B7CE9")
    
    net.add_node(sub, label=sub, color=sub_color)
    net.add_node(val, label=val, color=val_color)
    net.add_edge(sub, val, title=pred, label=pred)

# Enable physics layout for smooth interactive drag-and-drop
net.show_buttons(filter_=['physics'])
net.write_html("knowledge_graph.html")

print("[+] Graph saved to knowledge_graph.html. Open it in any web browser!")