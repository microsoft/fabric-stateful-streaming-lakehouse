# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aa7b8001-02f5-4fba-b3a1-8e94fd023e2f",
# META       "default_lakehouse_name": "sales_and_logistics",
# META       "default_lakehouse_workspace_id": "270c17cf-5177-4cd9-8120-e6256f546484",
# META       "known_lakehouses": [
# META         {
# META           "id": "aa7b8001-02f5-4fba-b3a1-8e94fd023e2f"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "19eab890-2e73-892e-40a1-04fe9b062f17",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Streaming Lakehouse with ArcFlow — Thinking Like a Software Engineer
# 
# > **Module 2 · Part 2** | ~30 min | Level 300
# 
# In Part 1 you built a raw → bronze → silver pipeline with plain PySpark. It works — but is it production-ready?
# 
# In this notebook you will take the **exact same transforms** and scale them through [**ArcFlow**](https://github.com/mwc360/ArcFlow/tree/main), an open-source PySpark streaming ELT framework. The goal is not to learn a specific tool, but to see how **packaging your code as software** changes everything:
# 
# | Principle | What it means for you |
# |-----------|----------------------|
# | 🧪 **Testability** | Validate transforms without writing to Delta — `test_input` / `test_output` use the memory sink pattern under the hood |
# | 📊 **Observability** | Health of every stream in one call via `controller.get_status()` |
# | 🔌 **Extensibility** | Add new tables by writing only business logic — zone names are arbitrary strings, not hardcoded layers |
# | 🔍 **Debuggability** | Automatic [Spark job descriptions](https://spark.apache.org/docs/latest/web-ui.html) in the Spark UI for every stage |
# | ⚡ **Event-driven declarative orchestration** | Downstream zones auto-trigger when upstream data lands — no polling, no manual sequencing |
# | ⚙️ **Best-practice configs** | `SparkConfigurator` auto-applies optimal Delta, AQE, and compression settings on init |
# 
# **Prerequisites** — Complete Part 1, the `explore_streaming` notebook. The `stream_all_zones` Spark Job Definition should already be running.
# 
# ---


# MARKDOWN ********************

# ## 1 — The Problem with Static Notebook Code
# 
# Think back to the bronze transform you built in Part 1. It worked perfectly for **one table**. Now imagine adding the rest:
# 
# | Entity | Format | Envelope | Custom Logic |
# |--------|--------|----------|-------------|
# | shipment_scan_event | [Eventstream](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/overview) (Kafka) | `_meta` + `data[]` | Explode + flatten 30+ fields |
# | shipment | JSON files | `_meta` + `data[]` | Flatten nested addresses |
# | order | JSON files | `_meta` + `data[]` | Explode `OrderLines[]` to line-level grain |
# | item | JSON files | `_meta` + `data[]` | Cast types, validate SKU |
# | customer | Parquet files | Flat | Flatten `DeliveryAddress` / `BillingAddress` structs |
# | route | Parquet files | Flat | Dimensional lookup table |
# | facility | Parquet files | Flat | Dimensional lookup table |
# | servicelevel | Parquet files | Flat | Dimensional lookup table |
# | exceptiontype | Parquet files | Flat | Dimensional lookup table |
# 
# With static notebook code, each table needs its own:
# - Schema definition
# - Reader configuration
# - Transform function
# - `writeStream` boilerplate (checkpoint, trigger, output table)
# - Monitoring code
# 
# **What breaks at scale:**
# 
# | Pain point | Why it matters |
# |-----------|----------------|
# | ❌ Duplicated boilerplate | Copy-paste errors compound across tables |
# | ❌ No testability | You must write to Delta to see if your transform works |
# | ❌ No versioning | The transform *is* the notebook — impossible to package or distribute |
# | ❌ Schema changes cascade | One upstream change means editing many cells |
# | ❌ No observability | Checking stream health means running separate cells per table |
# 
# ---


# MARKDOWN ********************

# ## 2 — Packaging Your ELT Framework
# 
# **ArcFlow** is a PySpark streaming ELT framework that separates *what* to process (configuration) from *how* to process (framework). Here is the conceptual split:
# 
# | You define (metadata) | Framework handles (machinery) |
# |----------------------|------------------------------|
# | Table name, schema | Reader selection (files, Kafka, EventHub) |
# | Source path or connection | [Checkpoint management](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing) |
# | Zone config (append vs. upsert) | `writeStream` orchestration |
# | Custom transform function name | [`foreachBatch`](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch) wiring |
# | Trigger mode | Stream lifecycle, error handling |
# | | **Best-practice Spark configs** (AQE, optimizeWrite, autoCompact, zstd compression) |
# 
# ---
# 
# > ⚙️ **Under the hood:** When the `Controller` initializes, `SparkConfigurator` auto-applies production-grade Spark settings — adaptive query execution, skew join handling, Delta auto-compaction, and more. It **never overwrites** configs you've already set on the session, and returns a summary of what was applied vs. skipped.
# 
# ### ⚖️ Side-by-Side: Boilerplate PySpark vs. ArcFlow
# 
# **Part 1 — Raw Spark:**
# ```python
# shipment_scan_event_schema=StructType(...)
# 
# # bronze
# bronze_sse_stream_df = (
#     spark.readStream
#     .format("kafka")
#     .options(**eventstream_options)
#     .option("startingOffsets", "earliest")
#     .option("failOnDataLoss", "false")
#     .load()
# )
# 
# bronze_sse_parsed_df = (
#     bronze_sse_stream_df.withColumn("value", sf.col("value").cast("string"))
#     .withColumn('value', sf.from_json(sf.col('value'), shipment_scan_event_schema))
#     .select('value.*')
#     .transform(normalize_columns_to_snake_case, shipment_scan_event_schema)
#     .selectExpr("_meta", "explode(data) as data")
#     .withColumn('_processing_timestamp', sf.current_timestamp())
# )
# 
# bronze_sse_stream_query = (bronze_sse_parsed_df.writeStream
#     .format('delta')
#     .option('checkpointLocation', 'Files/explore/checkpoints/bronze_sse')
#     .trigger(availableNow=True)
#     .toTable("dbo.bronze_sse")
# )
# 
# bronze_sse_stream_query.awaitTermination()
# 
# # silver
# def silver_transform(df) -> DataFrame:
#     df = df.select('data.*') \\
#         .withColumn('_processing_timestamp', sf.current_timestamp())
#     return df
# 
# silver_sse_stream_df = (
#     spark.readStream
#     .table('dbo.bronze_sse')
#     .transform(silver_transform)
# )
# 
# silver_sse_stream_query = (silver_sse_stream_df.writeStream
#     .format('delta')
#     .option('checkpointLocation', 'Files/explore/checkpoints/silver_sse')
#     .trigger(availableNow=True)
#     .toTable("dbo.silver_sse")
# )
# 
# silver_sse_stream_query.awaitTermination()
# ```
# 
# **ArcFlow — Python Config-driven:**
# ```python
# # Configuration (FlowConfig dataclass OR YAML)
# tables['shipment_scan_event'] = FlowConfig(
#     name='shipment_scan_event',
#     format='eventhub',
#     source_uri='Endpoint=sb://...',
#     schema=StructType(...),
#     zones={
#         'bronze': StageConfig(mode='append', custom_transform='explode_message_payload'),
#         'silver': StageConfig(mode='append', custom_transform='silver_shipment_scan_event'),
#     }
# )
# ```
# 
# **ArcFlow — YAML Config-driven:**
# ```yaml
# tables:
#     shipment_scan_event:
#         format: eventhub
#         source_uri: "Endpoint=sb://..."
#         schema: {'type': 'struct', 'fields': [{...}]}
#         zones:
#             bronze:
#                 mode: append
#                 custom_transform: explode_message_payload
#             silver:
#                 mode: append
#                 custom_transform: silver_shipment_scan_event
# ```
# 
# The ArcFlow configuration defines input sources and then the different zones that the data flows to with optional transformers. There's a single command to run all defined pipelines:
# 
# ```python
# # Execution — one line runs all tables through all zones
# controller.run_full_pipeline(zones=['bronze', 'silver'])
# ```
# 
# > The business logic (transforms) is identical. The boilerplate (readers, writers, checkpoints, triggers) disappears into the framework.
# 
# ---


# MARKDOWN ********************

# ## 3 — Add a New Object to the Framework
# 
# This is where the payoff becomes real. We will:
# 
# 1. Register the same transforms from Part 1 as **named transformer functions**
# 2. Use ArcFlow's **test methods** (memory sink) to validate without writing to Delta
# 3. Run the pipeline through the **controller** with automatic Spark job descriptions
# 4. Observe all streams at once with `controller.get_status()`
# 5. Wire up **new tables** by writing only business logic
# 
# Run the below to import the ArcFlow ELT framework:

# CELL ********************

from arcflow import Controller
from arcflow import FlowConfig, StageConfig, ZonePipeline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Start by creating a new YAML file in the Resources tab on the left pane:
# 1. Click **Resources**
# 1. Click the 3 dots `...` and choose **New file**
# 1. Name file as `pipeline.yml`
# 1. Expand the **Built-in** section and you will see your new file
# 1. Click the 2 dots `...` and choose **View and edit**
# 1. Paste the following YAML text:


# CELL ********************

# ── Global pipeline settings (passed to get_config) ──────────
config:
  streaming_enabled: true
  checkpoint_uri: "Files/arcflow_test/checkpoints/"
  landing_uri: "Files/landing/"
  trigger_mode: processingTime
  trigger_interval: 0 seconds
  
# ── Table registry ────────────────────────────────────────────
tables:
  shipment_scan_event:
    format: eventhub
    source_uri: Endpoint=sb://...
    schema: {'type': 'struct', 'fields': [{'name': '_meta', 'type': {'type': 'struct', 'fields': [{'name': 'enqueuedTime', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'producer', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'recordType', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'schemaVersion', 'type': 'string', 'nullable': True, 'metadata': {}}]}, 'nullable': True, 'metadata': {}}, {'name': 'data', 'type': {'type': 'array', 'elementType': {'type': 'struct', 'fields': [{'name': 'AdditionalData', 'type': {'type': 'struct', 'fields': [{'name': 'condition', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'loadId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'method', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'note', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'reason', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'reroute', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'resolution', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'review', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'signedBy', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'sortDecision', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'stopSequence', 'type': 'long', 'nullable': True, 'metadata': {}}, {'name': 'transportType', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'vehicleId', 'type': 'string', 'nullable': True, 'metadata': {}}]}, 'nullable': True, 'metadata': {}}, {'name': 'CurrentDestinationFacilityId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CurrentOriginFacilityId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CurrentServiceLevel', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'EmployeeId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'EstimatedArrivalTime', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'EventId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'EventTimestamp', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'EventType', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ExceptionCode', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ExceptionSeverity', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'FacilityId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'GeneratedAt', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'LocationLatitude', 'type': 'double', 'nullable': True, 'metadata': {}}, {'name': 'LocationLongitude', 'type': 'double', 'nullable': True, 'metadata': {}}, {'name': 'NextWaypointFacilityId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'OrganizationId', 'type': 'long', 'nullable': True, 'metadata': {}}, {'name': 'PlannedPathSnapshot', 'type': {'type': 'array', 'elementType': 'string', 'containsNull': True}, 'nullable': True, 'metadata': {}}, {'name': 'RelatedExceptionEventId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ResolutionAction', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RouteId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ScanDeviceId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ScheduleId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'SequenceNumber', 'type': 'long', 'nullable': True, 'metadata': {}}, {'name': 'ShipmentId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'SortLaneId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'SortingEquipmentId', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'TrackingNumber', 'type': 'string', 'nullable': True, 'metadata': {}}]}, 'containsNull': True}, 'nullable': True, 'metadata': {}}]}
    zones:
      bronze:
        mode: append
        schema_name: arcflow_bronze

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# Click the editor **save** button. Next, add the `shipment_scan_events` Event Hub connection string as the `source_uri` in your `pipeline.yml` file:
# 
# 1. Go to your **Fabric Workspace**
# 1. Open the **shipment_scan_events** Eventstream inside the `stateful-streaming-lakehouse` folder
# 1. Click the **Spark** _Destination_
# 1. With the _Event Hub_ protocol selected, click **SAS Key Authentication**
# 1. Click the **Show** icon next to **Connection string-primary key**
# 1. Click the **Copy** icon to save it to your clipboard
# 1. Return to the **arcflow_elt_framework** notebook and paste the connection string into `source_uri`, replacing `Endpoint=sb://...`. Ensure there is a space after the colon — it should look like `source_uri: Endpoint=sb://...`
# 1. Click the **save** icon in the file editor
# 
# > ⚠️ **Security note:** In production, store secrets in [Azure Key Vault](https://learn.microsoft.com/azure/key-vault/general/overview) and access them via Python. Inline connection strings are acceptable for demo purposes only.
# 
# Run the cell below to load your YAML configuration:


# CELL ********************

from arcflow import load_yaml_config, Controller, ZonePipeline

tables, dimensions, config = load_yaml_config("./builtin/pipeline.yml")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test the Raw Input
# 
# We only have one target zone (bronze) configured so far. Before writing anything to Delta, use `test_input` to **stream a sample into driver memory** and inspect the raw payload — the same development pattern you used in Part 1, now built into the framework.
# 
# Under the hood, `test_input` starts a streaming query with `trigger(availableNow=True)` writing to Spark's [`memory` sink](https://archive.apache.org/dist/spark/docs/3.5.2/structured-streaming-programming-guide.html#output-sinks) — no checkpoint, no Delta write, no cleanup. The `limit` parameter is applied at the broker level (`maxOffsetsPerTrigger`) so you only pull what you need.
# 
# Run the cell below with `raw=True` to see the Event Hub endpoint payload before schema application:


# CELL ********************

pipeline = ZonePipeline(
    spark=spark,
    zone="bronze",
    config={'streaming_enabled': True}
)
df_preview = pipeline.test_input(tables["shipment_scan_event"], raw=True, limit=10)
display(df_preview)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Now update the cell above: change `raw=True` to `raw=False` (or remove the parameter entirely) and re-run. Notice how the `body` column now has the schema applied — the framework handles deserialization automatically.
# 
# Next, run the cell below using `test_output` to see the data **as it would be written** to the target bronze Delta table. Expand the `body` column and notice how field names are automatically normalized to `snake_case` — no extra boilerplate, as it's built into the framework.


# CELL ********************

pipeline = ZonePipeline(
    spark=spark,
    zone="bronze",
    config={'streaming_enabled': True}
)
df_preview = pipeline.test_output(tables["shipment_scan_event"], limit=10)
display(df_preview)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Register Transformer Functions
# 
# In Part 1, transforms were loose functions in your notebook. In ArcFlow, you register them with the [`@register_zone_transformer`](https://github.com/mwc360/ArcFlow/tree/main?tab=readme-ov-file#2-define-custom-dataframe-transformers-optional) decorator so the framework can look them up by name from configuration.
# 
# The registry is a global dictionary — define your transform anywhere, register it once, and reference it by name in any YAML or `StageConfig`. This keeps configurations serializable (plain strings, not Python objects) and transforms discoverable.
# 
# ```python
# # The decorator uses the function name as the registry key
# @register_zone_transformer
# def my_transform(df: DataFrame) -> DataFrame: ...
# 
# # Or provide an explicit name
# @register_zone_transformer('custom_name')
# def my_transform(df: DataFrame) -> DataFrame: ...
# ```
# 
# Execute the cell below to register two transformers:


# CELL ********************

from pyspark.sql import DataFrame
from arcflow.transformations.zone_transforms import register_zone_transformer

# register user defined transformers (not packaged as part of the the ArcFlow library)
@register_zone_transformer
def explode_message_payload(df: DataFrame) -> DataFrame:
    # check whether body or value column exists and explode accordingly
    if 'body' in df.columns:
        df_expanded = df.select("body.*")
    elif 'value' in df.columns:
        df_expanded = df.select("value.*")
    else:
        raise ValueError("No 'body' or 'value' column found to explode in DataFrame")
    exploded_df = df_expanded.selectExpr("_meta", "explode(data) as data")
    return exploded_df

@register_zone_transformer
def silver_shipment_scan_event(df) -> DataFrame:
    import pyspark.sql.functions as sf
    df = (
        df.selectExpr(
            "_meta.*",
            "data.*",
        )
        .withColumn("enqueued_time", sf.to_timestamp("enqueued_time"))
        .withColumn("generated_at", sf.to_timestamp("generated_at"))
        .withColumn("event_timestamp", sf.to_timestamp("event_timestamp"))
        .withColumn("delivery_review", sf.col("additional_data").getItem("review"))
        .drop("_meta", "data", "_processing_timestamp")
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Now update `pipeline.yml` to add `custom_transform: explode_message_payload` to the bronze zone, then click **save** in the file editor.
# 
# Run the cell below to reload the configuration and see the updated bronze output with the exploded message payload applied:


# CELL ********************

# reload configuration
tables, dimensions, config = load_yaml_config("./builtin/pipeline.yml")

pipeline = ZonePipeline(
    spark=spark,
    zone="bronze",
    config={'streaming_enabled': True}
)
df_preview = pipeline.test_output(tables["shipment_scan_event"], limit=10)
display(df_preview)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test End-to-End: Bronze → Silver
# 
# Now add the silver zone to `pipeline.yml` by appending the following under the `zones:` key:
# 
# ```yaml
# silver:
#         mode: append
#         table_name: shipment_scan_event2
#         custom_transform: silver_shipment_scan_event
# ```
# 
# > ⚠️ YAML is parsed based on the position of strings. Make sure the pasted block vertically lines up with the bronze configuration.
# 
# Save the file, then run the cell below. Notice how the `zone` parameter is now set to `silver` — ArcFlow reads from the bronze output and applies the silver transformer, all **without writing intermediate Delta tables**. End-to-end development and testing without hitting storage:


# CELL ********************

# reload configuration
tables, dimensions, config = load_yaml_config("./builtin/pipeline.yml")

pipeline = ZonePipeline(
    spark=spark,
    zone="silver",
    config={'streaming_enabled': True}
)
df_preview = pipeline.test_output(tables["shipment_scan_event"], limit=10)
display(df_preview)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run the Full Pipeline
# 
# You've now validated the entire pipeline from raw → bronze → silver **without writing a single row to storage**. The same `memory` sink pattern from Part 1, but wrapped into reusable `test_input` / `test_output` methods.
# 
# Now run the real thing — the controller starts streaming queries for every configured table and zone in one call.
# 
# > ⚡ **Event-driven chaining:** By default, ArcFlow uses a [`StreamingQueryListener`](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#reporting-metrics-programmatically-using-asynchronous-apis) to cascade zones automatically. The upstream most zone (i.e. Bronze) runs with your configured trigger; when it produces data, the listener spawns silver as `availableNow` — no polling, no manual sequencing. If new data arrives while silver is already running, it queues a re-trigger automatically.


# CELL ********************

tables, dimensions, config = load_yaml_config("./builtin/pipeline.yml")
controller = Controller(spark, config, tables, dimensions)

controller.run_full_pipeline(zones=['bronze', 'silver'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 📊 Observe All Streams
# 
# In Part 1 you monitored one stream at a time with `query.status` and `query.lastProgress`. The controller gives you **every stream's health in a single call** — active state, rows processed, errors, batch duration, and more.
# 
# Also check the [Spark UI](https://spark.apache.org/docs/latest/web-ui.html) — ArcFlow automatically sets [`spark.sparkContext.setJobDescription()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.setJobDescription.html) for every stage, so you'll see descriptive labels like _"Running bronze for shipment_scan_event"_ instead of anonymous job IDs:


# CELL ********************

status_df = controller.get_status(as_dataframe=True)
display(status_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# When you're done exploring, stop all running streams with one call:


# CELL ********************

controller.stop_all()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Batch Mode with Stateful Processing
# 
# Remember the `availableNow` trigger from Part 1? Update `trigger_mode` to `availableNow` in your `pipeline.yml`, save, and re-run the pipeline below.
# 
# This runs a **batch job with streaming state** — it processes all available data since the last checkpoint and stops. You get incremental processing without a long-running stream. Schedule this from a [Data Factory Pipeline](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity) on any interval for use cases that don't require low-latency updates.
# 
# > 💡 **Tip:** In production Spark Job Definitions, set `await_termination: true` in your config so the job blocks until all `availableNow` queries finish. In notebooks, leave it `false` so your cells remain interactive.


# CELL ********************

tables, dimensions, config = load_yaml_config("./builtin/pipeline.yml")
controller = Controller(spark, config, tables, dimensions)

controller.run_full_pipeline(zones=['bronze', 'silver'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Check the controller status again until both streams show `'active': False` — they processed available data and stopped automatically.
# 
# > 💡 This is the same stateful behavior from Part 1: the checkpoint tracks what was already consumed. Re-run the cell above and it will complete almost instantly if there's no data to process, if there's data to process it will only process newly arrived data.


# CELL ********************

status_df = controller.get_status(as_dataframe=True)
display(status_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 — Explore the Production Pipeline
# 
# While you've been working through this notebook, the `stream_all_zones` Spark Job Definition has been running ArcFlow in the background — streaming **all 9 entities** through bronze and silver continuously via event-driven declarative orchestration.
# 
# Let's explore what's been produced. The queries below demonstrate:
# 
# | Query | What it shows |
# |-------|---------------|
# | **Bronze latency** | Time from source generation to bronze ingestion — how fast data lands |
# | **Silver latency** | End-to-end latency through both zones — the cost of each transformation step |
# | **Cross-table join** | Joining silver tables for analytics — the data is ready for dimensional modeling or ad-hoc queries |
# 
# > 🔍 **Notice:** No Delta configs are being set manually in these queries. The `SparkConfigurator` that ran at controller init auto-applied production-grade settings — [auto compaction](https://learn.microsoft.com/fabric/data-engineering/delta-optimization-and-v-order?tabs=sparksql#delta-table-maintenance), optimizeWrite, V-Order, zstd compression, and AQE — so you get optimal table performance without per-table tuning.
# 
# > 📡 **Why Eventstream over files?** For high-velocity use cases like shipment scan events, message brokers ([Fabric Eventstream](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/overview), Azure Event Hubs, Kafka) are preferred over file-based ingestion. They provide ordering guarantees, back-pressure handling, and significantly lower end-to-end latency than polling object storage. Compare the bronze latency below for `shipment_scan_event` (Eventstream) vs. the file-based entities.
# 
# > ⚠️ **File listing cost trap:** When streaming from object storage (OneLake, ADLS, S3), Spark must **list all files in the directory** on every trigger to determine what’s new. As files accumulate, this listing operation grows linearly and can become the dominant cost of your pipeline — even when there are zero new files to process. If you stream from files, use Spark’s built-in archival options to move processed files out of the landing directory:
# >
# > ```python
# > df = (spark.readStream
# >     .option('cleanSource', 'archive')
# >     .option('sourceArchiveDir', 'Files/archive/my_source/')
# >     ...
# > )
# > ```
# >
# > This keeps the landing directory small so listing stays fast. ArcFlow handles this automatically via the `archive_uri` config. Message brokers avoid the problem entirely — offset tracking is O(1) regardless of history depth.


# CELL ********************

# MAGIC %%sql
# MAGIC SELECT data.generated_at, _processing_timestamp, (unix_millis(_processing_timestamp) - unix_millis(cast(data.generated_at as timestamp))) / 1000.0 AS seconds_latency_from_source 
# MAGIC FROM bronze.order
# MAGIC group by all
# MAGIC order by cast(data.generated_at as timestamp) desc LIMIT 100

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT generated_at, _processing_timestamp, (unix_millis(_processing_timestamp) - unix_millis(generated_at)) / 1000.0 AS seconds_latency_from_source 
# MAGIC FROM silver.shipment_scan_event
# MAGIC group by all
# MAGIC order by generated_at desc LIMIT 100

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT i.item_id, i.description, i.brand, SUM(o.quantity) as quantity_sold, SUM(o.quantity * o.unit_price) as revenue
# MAGIC FROM silver.order o
# MAGIC JOIN silver.item i
# MAGIC     ON o.item_id = i.item_id
# MAGIC GROUP BY ALL
# MAGIC ORDER BY revenue desc
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 — Notebooks vs. Spark Job Definitions
# 
# You've been developing in a **Notebook**. The production pipeline runs as a [**Spark Job Definition (SJD)**](https://learn.microsoft.com/fabric/data-engineering/create-spark-job-definition). When should you graduate?
# 
# | | Notebook | Spark Job Definition |
# |---|---------|---------------------|
# | **Purpose** | Development, exploration, debugging | Production execution |
# | **Interactivity** | Cell-by-cell, visual output | Headless, scheduled or triggered |
# | **State** | Session-scoped | Runs to completion, restartable |
# | **Configuration** | Inline, ad-hoc | Source-controlled, versioned |
# | **Monitoring** | Manual cell execution | [Spark UI](https://spark.apache.org/docs/latest/web-ui.html), `get_status()` |
# | **Scaling** | Single session | Long-running, auto-restart capable |
# | **`await_termination`** | `false` — keep cells interactive | `true` — block until all streams finish |
# 
# ### Things to Consider
# 
# - **Testing and modularity** — Notebooks can blur setup, logic, and execution into one artifact, which can make unit testing and reuse harder. Packaging logic into a library (like ArcFlow) encourages separation between *what* the code does and *how* it runs.
# 
# - **Healthy friction** — SJDs require packaging, explicit interfaces, and parameterization (e.g. via `argparse`). That extra structure nudges you to think about what’s an input, what can change safely, and where validation lives.
# 
# - **Interactivity moves earlier** — You still explore and debug in notebooks. By the time you run an SJD, the code should already be tested. The SJD is just the execution contract.
# 
# **Rule of thumb:**
# - **Notebook** for building, testing, and one-off runs
# - **SJD** for anything that runs on a schedule or continuously
# 
# The transition is simple when your code is packaged: the SJD imports the same framework and config — no rewriting.
# 
# ### 📚 Further Reading
# 
# - [Notebooks vs. Spark Jobs in Production](https://milescole.dev/data-engineering/2026/02/04/Notebooks-vs-Spark-Jobs-in-Production.html) — a deeper look at the tradeoffs
# - [Creating Your First Spark Job Definition](https://milescole.dev/data-engineering/2026/02/04/Creating-your-first-Spark-Job-Definition.html) — step-by-step guide from notebook-first to SJD
# 
# ---


# MARKDOWN ********************

# ## 6 — Dimensional Modeling
# 
# The silver layer gives you clean, conformed data. The **gold layer** shapes it for analytics — [star schemas](https://learn.microsoft.com/fabric/data-warehouse/dimensional-modeling-overview), slowly changing dimensions (SCD2), and fact tables.
# 
# ### The Framework Advantage
# 
# SCD2 logic is complex — tracking history, managing surrogate keys, handling effective dates. Writing it once is hard enough. Writing it correctly for every dimension table is a maintenance burden.
# 
# **Without a framework:**
# ```python
# # Copy-paste this 50-line MERGE statement for every dimension table
# # Hope nobody introduces a subtle bug in copy #7
# # Good luck upgrading the SCD2 logic across all copies
# ```
# 
# **With a framework like ArcFlow** — you define a `DimensionConfig` that declares the source tables, business keys, SCD type, and a named transform. The framework handles the MERGE machinery:
# 
# ```python
# DimensionConfig(
#     name='dim_customer',
#     dimension_type='dimension',
#     source_tables=['customer', 'order'],   # multiple silver tables as input
#     source_zone='silver',
#     target_zone='gold',
#     scd_type='type2',                      # SCD1, SCD2, or SCD3
#     scd_key_columns=['customer_id'],
#     zone_config=StageConfig(
#         mode='upsert',
#         merge_keys=['customer_id'],
#     ),
#     transform='build_dim_customer',        # your business logic
# )
# 
# # The framework handles the SCD2 MERGE, surrogate keys, and effective dates
# controller.run_dimensions()
# ```
# 
# **The principle:** separate *what* an object is (your config) from *how* it is built (the framework). Don't repeat SCD2 logic — it's hard to maintain, hard to upgrade, and hard to audit.
# 
# ---


# MARKDOWN ********************

# ## 🏆 Key Takeaways
# 
# | Concept | What you practiced |
# |---------|--------------------|
# | **Static code → packaged framework** | Same transforms, but testable, versionable, distributable |
# | **`@register_zone_transformer`** | Name your transforms so config can reference them — keeps configs serializable |
# | **`test_input` / `test_output`** | Memory sink pattern wrapped as reusable methods — validate without writing to Delta |
# | **`controller.run_full_pipeline()`** | One call runs all tables through all zones with event-driven chaining |
# | **`controller.get_status()`** | Observe every stream's health in one call |
# | **Event-driven chaining** | Downstream zones auto-trigger via `StreamingQueryListener` — no polling |
# | **`SparkConfigurator`** | Production-grade Spark/Delta configs auto-applied at init — never overwrites your settings |
# | **Spark job descriptions** | Built-in debuggability in the Spark UI — every stage is labeled |
# | **Notebook → SJD** | Develop interactively, deploy as a scheduled job — same code, same config |
# | **Dimensional modeling** | Separate SCD2 machinery from business logic via `DimensionConfig` |
# 
# ### 📚 Further Reading
# 
# - [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# - [Fabric Spark Job Definitions](https://learn.microsoft.com/fabric/data-engineering/create-spark-job-definition)
# - [Delta Lake Streaming](https://docs.delta.io/latest/delta-streaming.html)
# - [Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
# - [ArcFlow on GitHub](https://github.com/mwc360/ArcFlow)
# 
# ### The Core Lesson
# 
# The transforms you wrote in Part 1 and Part 2 are **nearly identical**. The difference is how they are organized:
# 
# - Part 1: transforms live in notebook cells, wired together with manual boilerplate
# - Part 2: transforms are registered functions, wired together by a framework driven by configuration
# 
# When you package your ELT logic as software, you can **test it without side effects**, **version it with git**, **distribute it as a library**, and **hand it to a teammate** who only needs to write business logic.
# 
# > ArcFlow is the reference implementation — but the lessons transfer to any framework you build or adopt.

