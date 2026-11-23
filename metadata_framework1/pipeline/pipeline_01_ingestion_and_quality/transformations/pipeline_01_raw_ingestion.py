# pipeline_01_ingestion_and_quality.py
# --------------------------------------------------------------------------------------
# Purpose: Metadata-driven dynamic ingestion pipeline using Spark Declarative Pipelines.
# Behavior:
#   1. Reads configuration from Unity Catalog during DAG compilation.
#   2. Dynamically declares streaming tables with Liquid Clustering.
#   3. Dynamically binds Auto Loader append flows to target tables.
#   4. Retains declarative checkpointing and state management handled by DLT engine.
# --------------------------------------------------------------------------------------


from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ======================================================================================
# PHASE 1: DAG INITIALIZATION & METADATA HARVESTING
# ======================================================================================
# In Spark Declarative Pipelines / DLT, top-level Python executes once when the DAG is 
# built. We read our configuration Delta table to assemble table and flow manifests.

METADATA_TABLE = "ecom_dev.pipeline_config.ingestion_metadata"

# Read active configurations
config_rows = (
    spark.table(METADATA_TABLE)
    .filter(F.col("is_active") == True)
    .collect()
)

# Organize rows into an in-memory hierarchy:
# {
#    "target_table": {
#        "cluster_keys": ["country", "customer_id"],
#        "flows": [ { flow_definition }, ... ]
#    }
# }
tables_manifest = {}

for row in config_rows:
    t_name = row["target_table"]
    
    # Initialize table entry if not present
    if t_name not in tables_manifest:
        raw_cluster_keys = row["cluster_keys"]
        cluster_list = (
            [k.strip() for k in raw_cluster_keys.split(",") if k.strip()]
            if raw_cluster_keys
            else []
        )
        tables_manifest[t_name] = {
            "cluster_by": cluster_list,
            "flows": []
        }
    
    # Append the flow definition to the target table
    tables_manifest[t_name]["flows"].append({
        "flow_name": row["flow_name"],
        "source_path": row["source_path"],
        "source_format": row["source_format"],
        "channel": row["channel"],
        "schema_ddl": row["schema_ddl"]
    })

    # ======================================================================================
# PHASE 2: DYNAMIC STREAMING TABLE DECLARATIONS
# ======================================================================================
# We declare target streaming tables outside of any flow. 
# dp.create_streaming_table tells the declarative engine to reserve the Delta table 
# contract, enforce liquid clustering, and prepare internal checkpoints.

for target_table_name, target_config in tables_manifest.items():
    dp.create_streaming_table(
        name=target_table_name,
        comment=f"Unified Bronze streaming table for target: {target_table_name}",
        cluster_by=target_config["cluster_by"]
    )


# ======================================================================================
# PHASE 3: FLOW FACTORY (PREVENTS PYTHON CLOSURE / LATE-BINDING TRAP)
# ======================================================================================
# Gotcha: If you use @dp.append_flow directly inside a for-loop, Python captures 
# loop variables by reference, causing all flows to execute with the last row's values.
# Wrapping the declaration in an isolated factory function freezes variable scope per flow.

def register_append_flow(
    target_table: str,
    flow_name: str,
    source_path: str,
    source_format: str,
    channel: str,
    schema_ddl: str
):
    """
    Registers an Auto Loader append flow targeting a unified streaming table.
    """
    @dp.append_flow(target=target_table, name=flow_name)
    def ingestion_flow():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source_format)
            # Schema Evolution: Rescues unknown/malformed attributes safely into _rescued_data
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            # PySpark natively parses standard DDL strings directly:
            .schema(schema_ddl)
            .load(source_path)
            # Lineage & Audit metadata enrichment:
            .withColumn("source_channel", F.lit(channel))
            .withColumn("ingest_ts", F.current_timestamp())
        )


# ======================================================================================
# PHASE 4: DYNAMIC FLOW REGISTRATION
# ======================================================================================
# Iterate through each table manifest and invoke our factory to wire the ingestion streams.

for target_table_name, target_config in tables_manifest.items():
    for flow in target_config["flows"]:
        register_append_flow(
            target_table=target_table_name,
            flow_name=flow["flow_name"],
            source_path=flow["source_path"],
            source_format=flow["source_format"],
            channel=flow["channel"],
            schema_ddl=flow["schema_ddl"]
        )
