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
# META     },
# META     "event_stream": {
# META       "known_event_streams": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# <div style="margin: 0; padding: 0; text-align: left;">
#   <table style="border: none; margin: 0; padding: 0; border-collapse: collapse;">
#     <tr>
#       <td style="border: none; vertical-align: middle; text-align: left; padding: 0; margin: 0;">
#         <img src="https://raw.githubusercontent.com/microsoft/fabric-analytics-roadshow-lab/main/assets/images/spark/analytics.png" width="140" />
#       </td>
#       <td style="border: none; vertical-align: middle; padding-left: 0px; text-align: left; padding-right: 0; padding-top: 0; padding-bottom: 0;">
#         <h1 style="font-weight: bold; margin: 0;">Stateful Streaming Lakehouse</h1>
#       </td>
#     </tr>
#   </table>
# </div>
# 
# # Explore Streaming — Core Concepts with Apache Spark
# 
# In this notebook you will go hands-on with **Spark Structured Streaming** using plain PySpark — no frameworks, no abstractions. By the end you will have built a working **raw → bronze → silver** streaming pipeline and understand the primitives that every production streaming system is built on.
# 
# ### How Data Arrives
# 
# | Entity | Landing Format | Path / Endpoint |
# |--------|---------------|----------------|
# | item | JSON | `Files/landing/item/` |
# | order | JSON | `Files/landing/order/` |
# | shipment | JSON | `Files/landing/shipment/` |
# | customer | Parquet | `Files/landing/customer/` |
# | route | Parquet | `Files/landing/route/` |
# | facility | Parquet | `Files/landing/facility/` |
# | servicelevel | Parquet | `Files/landing/servicelevel/` |
# | exceptiontype | Parquet | `Files/landing/exceptiontype/` |
# | **shipment_scan_event** | **[Eventstream](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/overview)** ([Kafka protocol](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-source-custom-app#kafka)) | Fabric Eventstream endpoint |
# 
# JSON files follow an envelope pattern: `{"_meta": {...}, "data": [...]}`. Parquet files are flat with `OrganizationId` and `GeneratedAt` columns added by the data generator.
# 
# > ℹ️ _In production, high-velocity events like shipment scan events are best to stream to a message store like [Fabric Eventstream](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/overview) of Azure EventHub. Given that Data Engineers don't always get the choice of how to receive data, we will also be processing files being streamed to OneLake in the `Files/landing/` directory._
# 
# ### The Target Schema
# By the end of the lab, you'll understand some basic concepts and then see the outcome of a mature data engineering pipeline:
# 
# ![McMillian Industrial Group Silver Schema](https://raw.githubusercontent.com/microsoft/fabric-analytics-roadshow-lab/main/assets/images/spark/silver-erd.png)
# 
# 
# ---


# MARKDOWN ********************

# # Prerequisite Step
# Before we explore Spark fundamentals, you need to **start the production-grade Spark Job Definition** that will both generate our synthetic data and process the input data throughout of this lab.
# 


# MARKDOWN ********************

# ## 🎯 Step 1: Get the Eventstream connection strings
# 
# You will need **two connection strings** from the Eventstream:
# 
# - **Producer connection string** → from **PackageScanners** custom endpoint
# - **Consumer connection string** → from **Spark** custom endpoint
# 
# ### 1. Copy the Producer Connection String
# 
# 1. In the **Fabric Workspace** (left navigation), open the `stateful-streaming-lakehouse` folder.  
# 1. Open the **shipment_scan_events** **Eventstream**.  
# 1. In the stream diagram, select the **PackageScanners** endpoint.  
# 1. Ensure the **Event Hub protocol** is selected.  
# 1. Click **Show** next to **Connection string-primary key**.  
# 1. Click **Copy** to copy the connection string.
# 1. **Paste** as the value for the `producer_connection_string` Python variable in the cell below
# 
# ### 2. Copy the Consumer Connection String
# 
# 1. In the stream diagram, select the **Spark** destination.  
# 1. Ensure the **Event Hub protocol** is selected.  
# 1. Click **Show** next to **Connection string-primary key**.  
# 1. Click **Copy** to copy the connection string.
# 1. **Paste** as the value for the `consumer_connection_string` Python variable in the cell below


# CELL ********************

producer_connection_string = "<PASTE PackageScanners connection string>"
consumer_connection_string = "<PASTE Spark connection string>"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🎯 Step 2: Populate SJD Command Line Arguments
# 
# 1. Run the below cell to form the command line arguments for our Spark Job Definition
# 1. **Copy** the output
# 1. Go to the [stream_all_zones](https://app.powerbi.com/groups/$workspaceId/sparkjobdefinitions/$sparkJobDefinitionId?experience=fabric-developer) Spark Job Definition
# 1. **Paste** the value into the `Command line arguments` field
# 1. Click **Save** (top header ribbon)
# 1. Click **Run** (top header ribbon)
# 1. _After waiting 2-3 seconds_, expand the **Runs** tab at the bottom of the page
# 1. Click the **Refresh** icon and you will see your Spark Job run has started
# 1. Return to this notebook and move on to the next section
# 


# CELL ********************

print(f"--producer-connection-string {producer_connection_string} --consumer-connection-string {consumer_connection_string}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ⏳ Waiting for Streaming Data
# 
# 
# > **Run the next cell** and grab a sip of coffee ☕ — the below waits until our stream produces its first output file.  
# Once it completes successfully, you can proceed with the rest of the lab. This should take 3-5 minutes.

# CELL ********************

import time

print('Waiting for tutorial files to start landing...')
files = []
start_time = time.time()

while True:
    if time.time() - start_time > 600:
        raise TimeoutError("Timed out waiting for files in Files/archive/item after 10 minutes. Make sure `stream_bronze_and_silver` Spark Job Definition was triggered!")
    time.sleep(5)
    try:
        files = notebookutils.fs.ls('Files/archive/item')
    except Exception as e:
        if "FileNotFoundException" in str(e):
            continue
        else:
            raise
    if len(files) > 0:
        print('Ready to proceed!')
        break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🌊 Why Structured Streaming?
# 
# **Structured Streaming** is Spark's powerful engine for processing data streams, but it's useful far beyond just real-time, low-latency scenarios. Here's why it's commonly used in modern data engineering:
# 
# ### 🎯 Key Benefits
# 
# 1. **Built-in Incremental Processing**
#    - Automatically tracks which data has been processed
#    - Only processes new/changed files since the last run
#    - No need to manually manage watermarks or state
# 
# 1. **Exactly-Once Semantics**
#    - Guarantees each record is processed exactly once
#    - Prevents duplicate data in your Delta tables
#    - Handles failures gracefully with automatic recovery
# 
# 1. **Fault Tolerance**
#    - Checkpointing saves progress automatically
#    - If a job fails, it resumes from the last checkpoint
#    - No data loss or reprocessing of already-handled records
# 
# 1. **Unified API**
#    - Same DataFrame API for batch and streaming
#    - Write once, run in batch or streaming mode
#    - Easy to prototype in batch, deploy as streaming
# 
# 1. **Optimized for Delta Lake**
#    - Native integration with Delta tables
#    - Handles schema evolution automatically
#    - Enables time travel and data versioning
# 
# ### 💼 Common Use Cases
# 
# - **ETL Pipelines**: Continuously ingest and transform data as it arrives
# - **Data Lakehouse**: Build incremental Bronze → Silver → Gold pipelines
# - **Real-time Analytics**: Power dashboards with up-to-the-minute data
# - **Change Data Capture (CDC)**: Process CDC data from source systems
# - **Event Processing**: Handle IoT sensors, clickstreams, logs, etc.
# 


# MARKDOWN ********************

# 
# ## 1 — Why Structured Streaming for a Batch Pipelines?
# 
# [Structured Streaming](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html) is not just for "real-time" data requirements — it is often the engine of choice for **incremental batch** pipelines too. Two features make it ideal for typical batch architectures:
# 
# | Property | What it means |
# |----------|---------------|
# | **Checkpoint state** | Spark remembers which files (or offsets) it has already processed. Re-run the same job and it picks up only *new* data — no custom bookmarks required. |
# | **Batch trigger** | The `availableNow` trigger will process everything that has accumulated since the last run, then stop. You get **batch scheduling semantics** with **streaming state management**. See the [docs](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#triggers) for more info on triggers. |
# 
# <br>
# 
# > Think of `availableNow` as a batch job that *also* remembers where it left off. No need to build external mechanics to manage state.


# MARKDOWN ********************

# ## 2 — Structured Streaming Fundamentals
# 
# Structured Streaming is Spark's **scalable and fault-tolerant** stream processing engine. It treats streaming data as an **unbounded table** that grows continuously.
# 
# ### 🧩 Key Streaming Concepts
# 
# | Component | Description |
# |-----------|-------------|
# | **Input Source** | Where data comes from (files, Kafka, Event Hubs, etc.) |
# | **Transformations** | How you process each micro-batch (same API as batch!) |
# | **Output Sink** | Where results are written (Delta tables, console, memory, etc.) |
# | **Checkpointing** | Tracks progress for exactly-once processing and fault tolerance |
# | **Trigger Intervals** | How often to process new data (continuous, fixed interval, available now) |
# 
# ### 🔧 The Streaming Pattern
# 
# ```python
# # 1. Read stream from source
# df = spark.readStream.format("json").load("path/to/input")
# 
# # 2. Apply transformations (same as batch!)
# transformed = df.select(...).where(...).withColumn(...)
# 
# # 3. Write to Delta Lake
# query = transformed.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", "path/to/checkpoint") \
#     .start("path/to/delta/table")
# ```
# 
# ### 💡 Batch vs Streaming: Same Code!
# 
# The beauty of Structured Streaming is that **the same transformation code** works for both batch and streaming. The only difference is:
# - Batch: `spark.read...` → `df.write...`
# - Streaming: `spark.readStream...` → `df.writeStream...`
# 
# ### Side-by-Side: Batch Read vs. Streaming Read
# 
# | | `spark.read` | `spark.readStream` |
# |---|---|---|
# | Returns | Static `DataFrame` | Streaming `DataFrame` |
# | Schema | Optional (can infer) | **Required** |
# | Data scope | All data at call time | Only *new* data per trigger |
# 
# 
# The code is nearly identical — the only difference is `read` → `readStream` and the requirement of specifying the **schema**.
# 
# > ℹ️ **Note:** we are reading from the `archive` directory because our `stream_bronze_and_silver` Spark Job Definition is processing the files in the `landing` directory in near real-time and then moving processed files to the `archive` directory. This gives us a stable directory to read from.
# 
# Execute the cell below to read all customer parquet files in one batch:


# CELL ********************

# --------- Batch API - Create DataFrame ---------
# Reads all customer parquet files at once (full scan every time)
customer_batch_df = spark.read.parquet(
    'Files/archive/customer',
    recursiveFileLookup=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Now execute the streaming version. Notice the one addition: **schema**:

# CELL ********************

# --------- Streaming API - Create DataFrame ---------
# Reads only NEW customer parquet files since the last checkpoint
customer_stream_df = spark.readStream.schema(
    customer_batch_df.schema          # streaming requires an explicit schema
).parquet(
    'Files/archive/customer',
    recursiveFileLookup=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Side-by-Side: Batch Write vs. Streaming Write
# 
# The `writeStream` API has more differences but still follows similar primatives as the `write` batch API.
# - `format`
# - `option`s
# 
# Run the below and compare the differences between the code:

# CELL ********************

# --------- Batch API - Write DataFrame ---------
customer_batch_df.write.mode('append').saveAsTable('dbo.batch_customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Run the below streaming write query and notice the few differences:
# - `saveAsTable` (batch) vs. `toTable` (streaming)
# - `checkpointLocation` -> this is where the state of the query is managed
# - `availableNow` trigger -> this specifies that they query should run to process available data and then stop
# - `query.awaitTermination()` -> if not executed, this would allow our streaming query to run **asynchronously**. By providing this we are telling the driver to wait for the query to complete, a.k.a. **synchronous** processing. 

# CELL ********************

# --------- Streaming API - Write DataFrame ---------
query = (customer_stream_df.writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', 'Files/explore/checkpoints/customer')
    .trigger(availableNow=True)
    .toTable('dbo.stream_customer')
)

query.awaitTermination()
print('Stream completed — only new files since the last checkpoint were processed.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# > 🔄 **Impact of stateful API:** Run the above batch and streaming cells **a second time**. The stream only processes new data — the checkpoint tracked what was already consumed. The batch job appended the same data into the `batch_customer` table since there is no state that is managed. We now have duplicate records because we didn't have a process to manage state!

# CELL ********************

# MAGIC %%sql
# MAGIC WITH batch_table_count as (
# MAGIC     SELECT COUNT(1) as record_count, 'batch_customer' as table_name FROM dbo.batch_customer
# MAGIC ),
# MAGIC stream_table_count as (
# MAGIC     SELECT COUNT(1) as record_count, 'stream_customer' as table_name FROM dbo.stream_customer
# MAGIC )
# MAGIC SELECT * from batch_table_count
# MAGIC UNION ALL 
# MAGIC SELECT * from stream_table_count

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 🔄 Convert Batch to Streaming
# 
# With file based sources we can start with batch and then convert to streaming for managing incremental processing. After validating the output, we can **simply switch** to the `readStream` API! The transformation logic remains identical, except for the need to specify the schema of the input DataFrame.
# 
# **Key Changes:**
# 1. `spark.read` → `spark.readStream`
# 1. Schema can be implicit or defined → `.schema()` is required for streaming operations
# 1. `df.write` → `df.writeStream` (add checkpoint location and trigger)

# MARKDOWN ********************

# ## 3 — Write Streaming Queries on Top of Files
# 
# Now let's read **JSON** files. The shipment data follows a common envelope pattern from event stores and APIs:
# 
# ```json
# {
#   "_meta": { "schemaVersion": "1.0", "producer": "lakegen.mcmillan", "recordType": "shipment", "enqueuedTime": "..." },
#   "data": [ { "ShipmentId": "...", "TrackingNumber": "...", ... } ]
# }
# ```
# 
# Execute the cell below to infer the schema from existing files.
# 
# > 💡 In production you should define schemas explicitly — depending on the data type, schema inference can result in all input data being scanned twice (i.e. CSV files) and can make data pipelines much slower. See [Spark schema specification](https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option) and the blog [Should You Infer Schema in Production?](https://milescole.dev/data-engineering/2026/02/20/Infer-Schema-In-Production.html). While JSON does have a `samplingRatio` option, it still adds overhead and adds risk by not having an explicit contractual expectation for your input data must land for it to be processed.


# CELL ********************

shipment_schema = spark.read.json(
    'Files/archive/shipment',
    multiLine=True,
    recursiveFileLookup=True
).schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Notice how long that took to get the schema of the folder with JSON files. Depending on the file type being read, all data may need to be scanned an initial time just to infer the schema, and then would scanned a second time when an action is performed on the DataFrame.
# 
# > ⚠️ While schema inference is fine when doing development, production pipelines should avoid it. Instead consider the following options:
# > 1. Add a static `struct` object to your code (i.e. below)
# > 1. Commit a small sample payload of the expected schema into your project and then infer schema from this singular file 


# CELL ********************

from pyspark.sql.types import *
schema = StructType([StructField('_meta', StructType([StructField('enqueuedTime', StringType(), True), StructField('producer', StringType(), True), StructField('recordType', StringType(), True), StructField('schemaVersion', StringType(), True)]), True), StructField('data', ArrayType(StructType([StructField('CommittedDeliveryDate', StringType(), True), StructField('CurrentFacilityId', StringType(), True), StructField('CustomerId', StringType(), True), StructField('CustomerName', StringType(), True), StructField('DeclaredValue', DoubleType(), True), StructField('DestinationAddress', StringType(), True), StructField('DestinationCity', StringType(), True), StructField('DestinationCountry', StringType(), True), StructField('DestinationFacilityId', StringType(), True), StructField('DestinationLatitude', DoubleType(), True), StructField('DestinationLongitude', DoubleType(), True), StructField('DestinationState', StringType(), True), StructField('DestinationZipCode', StringType(), True), StructField('Distance', DoubleType(), True), StructField('GeneratedAt', StringType(), True), StructField('Height', DoubleType(), True), StructField('IsFragile', BooleanType(), True), StructField('IsHazardous', BooleanType(), True), StructField('LateDeliveryPenaltyPerDay', DoubleType(), True), StructField('Length', DoubleType(), True), StructField('OrderId', StringType(), True), StructField('OrderLineList', ArrayType(LongType(), True), True), StructField('OrganizationId', LongType(), True), StructField('OriginAddress', StringType(), True), StructField('OriginCity', StringType(), True), StructField('OriginCountry', StringType(), True), StructField('OriginFacilityId', StringType(), True), StructField('OriginLatitude', DoubleType(), True), StructField('OriginLongitude', DoubleType(), True), StructField('OriginState', StringType(), True), StructField('OriginZipCode', StringType(), True), StructField('RequiresRefrigeration', BooleanType(), True), StructField('ServiceLevel', StringType(), True), StructField('ShipDate', StringType(), True), StructField('ShipmentId', StringType(), True), StructField('SpecialInstructions', StringType(), True), StructField('TrackingNumber', StringType(), True), StructField('Volume', DoubleType(), True), StructField('Weight', DoubleType(), True), StructField('Width', DoubleType(), True)]), True), True)])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Now start the stream of the shipment files to a Delta table and move to the next step:

# CELL ********************

shipment_stream_df = spark.readStream.schema(shipment_schema).json(
    'Files/archive/shipment',
    multiLine=True,
    recursiveFileLookup=True
)

shipment_query = (shipment_stream_df.writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', 'Files/explore/checkpoints/shipment_raw')
    .trigger(availableNow=True)
    .toTable('dbo.shipment')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 📊 Observing a Stream
# 
# Every [`StreamingQuery`](https://archive.apache.org/dist/spark/docs/3.5.2/structured-streaming-programming-guide.html#managing-streaming-queries) exposes two properties to monitor the query which are useful when running asynchronous streaming queries:
# 
# | Property | Purpose |
# |----------|---------|
# | `query.status` | Is the stream running, processing data, or stopped? |
# | `query.lastProgress` | Metrics from the most recent micro-batch — input rows/sec, processed rows/sec, batch duration. |
# 
# Run the cell below to inspect the stream you just ran:

# CELL ********************

print('=== status ===')
print(shipment_query.status)

print('\n=== lastProgress (key fields) ===')
lp = shipment_query.lastProgress
if lp:
    print(f'  batchId          : {lp["batchId"]}')
    print(f'  numInputRows     : {lp["numInputRows"]}')
    print(f'  inputRowsPerSec  : {lp["inputRowsPerSecond"]}')
    print(f'  processedRowsSec : {lp["processedRowsPerSecond"]}')
    print(f'  batchDurationMs    : {lp["durationMs"]}')

print('\n=== lastProgress (full object) ===')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ Verify Your Streaming Pipeline
# 
# Once the `status` shows as **"Stopped"** or `lastProgress` returns metrics, your streaming job has completed!
# 
# **📂 Verify the Table Was Created:**
# 1. Look at the **Lakehouse explorer** on the left sidebar
# 2. Expand the **Tables** section
# 3. Find the `shipment` table under the `dbo` schema
# 4. Right-click and select **Load data -> Spark**, drag and drop the table onto your Notebook, or query it with SparkSQL!
# 
# > 🎉 **Congratulations!** You've just observed your first Spark Structured Streaming pipeline in Microsoft Fabric!
# 
# ---

# MARKDOWN ********************

# ### 🎯 Challenge: Query the Shipment Table
# 
# Write a SQL query against the `dbo.shipment` table you just created. Use `EXPLODE(data)` to break the `data` array into individual rows and then expand the struct fields with `.*`.
# 
# Try it in the cell below!

# CELL ********************

# MAGIC %%sql


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# <details>
#   <summary><strong>🔑 Solution:</strong> Click to reveal the answer</summary>
# 
# <br/>
# 
# **Approach:**
# 1. Inner query: `EXPLODE(data)` converts the array into rows with alias `shipment`
# 2. Outer query: `shipment.*` expands all struct fields into columns
# 
# ```sql
# SELECT shipment.*
# FROM (
#     SELECT explode(data) as shipment 
#     FROM dbo.shipment
# );
# ```
# 
# **Key Takeaway:** This two-step pattern (explode → expand) is fundamental for flattening nested JSON in data engineering pipelines.
#   
# </details>

# MARKDOWN ********************

# ## 4 — Core Streaming API Semantics
# 
# This section covers the key building blocks you combine in every streaming pipeline. See the [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html) for the full reference.
# 
# ### Triggers
# 
# The **trigger** controls *when* and *how often* a micro-batch executes:
# 
# | Trigger | Behaviour |
# |---------|----------|
# | [`trigger(availableNow=True)`](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#triggers) | Process all available data, then **stop**. Best for scheduled batch-style runs. |
# | `trigger(processingTime='30 seconds')` | Continuously run a micro-batch every 30 s. |
# | `trigger(once=True)` | Legacy — same intent as `availableNow` but processes in a single batch (less efficient). |
# 
# ### `awaitTermination`
# 
# `query.awaitTermination()` **blocks** the calling cell until the stream finishes. Use it when:
# - You run `availableNow` and want downstream cells to wait
# - You orchestrate multiple sequential streams in one notebook
# 
# Without it, the stream runs **asynchronously** and the next cell executes immediately.
# 
# ### Fan-Out with `foreachBatch`
# 
# [`foreachBatch`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html) gives you a **regular DataFrame** for each micro-batch, letting you write to **multiple sinks** or apply arbitrary logic:
# 
# ```python
# def write_to_multiple_tables(batch_df, batch_id):
#     # write 1
#     batch_df.write.format('delta').mode('append').saveAsTable('bronze.events')
#     alerts = batch_df.where("severity = 'CRITICAL'")
#     # write 2
#     alerts.write.format('delta').mode('append').saveAsTable('bronze.alerts')
# 
# query = (stream_df.writeStream
#     .foreachBatch(write_to_multiple_tables)
#     .option('checkpointLocation', '...')
#     .trigger(availableNow=True)
#     .start()
# )
# ```
# 
# > `foreachBatch` turns each micro-batch into a plain batch operation where any DataFrame API call works.
# 
# ---


# MARKDOWN ********************

# ## 5 — Build a Raw → Bronze Streaming Pipeline
# 
# A good bronze layer applies three things to every source:
# 
# | Step | What it does |
# |------|--------------|
# | **Schema enforcement** | Parse the raw payload with a defined schema — no surprises downstream |
# | **Column normalization** | Convert source column names to `snake_case` — defensive against inconsistent or problematic names from upstream systems |
# | **Audit columns** | Add `_processing_timestamp` so you know when each row was ingested |
# 
# > 🛡️ **Why normalize in bronze?** Source systems are unpredictable — column names may arrive as `PascalCase`, `camelCase`, contain spaces, or mix conventions across producers. Normalizing early means every downstream consumer (silver, gold, ad-hoc queries) gets a consistent contract from day one. Fixing column names later requires coordinating changes across every table and consumer that already references them.
# 
# ### 🧪 The Development Challenge
# 
# > How do you develop against a live stream without writing garbage to your Delta tables?
# 
# **Answer:** Use the [**`memory` sink**](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#output-sinks). It writes micro-batch output to an in-memory temporary view — no checkpoints, no Delta writes, no cleanup.
# 
# Let's peek at the raw order JSON first:


# CELL ********************

# Peek at raw order JSON — notice the _meta + data[] envelope
raw_order = spark.read.json(
    'Files/archive/order',
    multiLine=True,
    recursiveFileLookup=True
).limit(10)
display(raw_order)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Stream a Sample to Memory
# 
# Execute the cell below to stream order data into driver **memory** with a queriable view — no Delta writes, no checkpoint:

# CELL ********************

order_file_schema = raw_order.schema

mem_query = (spark.readStream
    .schema(order_file_schema)
    .json('Files/archive/order', multiLine=True, recursiveFileLookup=True)
    .writeStream
    .format('memory')
    .queryName('order_preview')
    .trigger(availableNow=True)
    .start()
)
mem_query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 🎯 Challenge: Query the Order Table
# 
# Query the `order_preivew` temporary view just like you would with a table.
# 
# > **TIP**: use `LIMIT` to make the result return much faster!

# CELL ********************

# MAGIC %%sql


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# <details>
#   <summary><strong>🔑 Solution:</strong> Click to reveal the answer</summary>
# 
# <br/>
# 
# **Approach:**
# 1. Inner query: `EXPLODE(data)` converts the array into rows with alias `shipment`
# 2. Outer query: `shipment.*` expands all struct fields into columns
# 
# ```sql
# SELECT * FROM order_preview LIMIT 10
# ```
# 
# **Key Takeaway:** This two-step pattern (explode → expand) is fundamental for flattening nested JSON in data engineering pipelines.
#   
# </details>
# 
# > 🚀 The `memory` sink is your best friend during development. You get a queryable view without touching storage beyond the initial stream into memory!

# MARKDOWN ********************

# Alternatively, you can also change `readStream` to `read` but the downside is that the stream specific semantics need to be removed:

# CELL ********************

order_file_schema = raw_order.schema

df = (spark.read
    .schema(order_file_schema)
    .json('Files/archive/order', multiLine=True, recursiveFileLookup=True)
)
df.createOrReplaceTempView("order_preview")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Eventstream Source Pattern (Kafka)
# 
# In production, `shipment_scan_event` streams through **Fabric Eventstream** using the Kafka protocol. The read pattern looks like this:
# 
# ---
# 
# ```python
# kafka_df = (spark.readStream
#     .format('kafka')
#     .option('kafka.bootstrap.servers', '<eventstream-endpoint>:9093')
#     .option('kafka.sasl.mechanism', 'PLAIN')
#     .option('kafka.security.protocol', 'SASL_SSL')
#     .option('kafka.sasl.jaas.config',
#             'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
#             'required username="$ConnectionString" '
#             'password="<connection-string>";')
#     .option('subscribe', '<topic>')
#     .option('startingOffsets', 'earliest')
#     .load()
# )
# 
# # The value column is a binary JSON payload — parse the _meta + data[] envelope
# parsed = kafka_df.select(
#     from_json(col('value').cast('string'), sse_envelope_schema).alias('payload')
# ).select('payload.*')
# ```
# 
# ---
# 
# ##### 🎯 To simplify reading from the Eventstream, add an Eventstream connection via the _Data items_ pane on the left:
# 1. Make sure the **Data items** tab is selected in the left-side _Explorer_
# 1. Click **Add data items**
# 1. Choose **From Real-Time Hub**
# 1. Select the Eventstream named `shipment_scan_events`
# 1. Click **Next**
# 1. Click **Finish**
# 1. Expand the `shipment_scan_events` Eventstream that now shows in the Object Explorer
# 1. Click the 3 dots `...` next to `shipment_scan_events-stream`, and then choose **Read with Spark**.
# 
# It will add 2 cells. **Delete the second one that has the readStream operation.** We only need to **keep the one that looks like the below**:
# 
# ```python
# # Set the event stream item and datasource IDs
# __in_eventstream_item_id = "00000000-0000-0000-0000-000000000000"
# __in_eventstream_datasource_id = "00000000-0000-0000-0000-000000000000"
# ```


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Execute the cell below to stream a small sample from the Eventstream into an in-memory temp view so we can inspect the raw Kafka payload:


# CELL ********************

eventstream_options = {
    "eventstream.itemid": __in_eventstream_item_id,
    "eventstream.datasourceid": __in_eventstream_datasource_id
}

shipment_scan_event_df = (
    spark.read
    .format("kafka")
    .options(**eventstream_options)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 5)
    .load()
    .limit(4)
)

display(shipment_scan_event_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 🎯 Challenge: Cast, Schematize, Expand, and Explode JSON
# 
# This data is far from usable. The `value` field:
# - is a _binary_ value so we can't see the actual structure
# - after casting to a string, we'd still just have a _string_ — not a **struct** that enables us to expand, drill, explode, etc.
# - contains an embedded `_meta` and `data` field
# - the `data` field is an array where each element contains up to 100 complex records
# - each `shipment_scan_event` record inside the `data` array needs to be expanded into its own row
# 
# See how much of the DataFrame you can parse using the schema struct defined below. Don't worry — the next section walks through each transformation step-by-step if you aren't able to complete it. The goal is a **flattened DataFrame** where each `shipment_scan_event` is its own row with all fields as top-level columns.
# 
# > **Tip 1** Avoid reusing prior DataFrame variable names like `shipment_stream_df` or overusing `df` when chaining transformations. Use distinct names at each step to avoid accidentally overriding an earlier assignment.
# 
# > **Tip 2** PySpark function are already aliased as `sf` (spark functions). If it was not already imported you would add `import pyspark.sql.functions as sf`


# CELL ********************

import pyspark.sql.functions as sf

shipment_scan_event_schema=StructType( [ StructField( "_meta", StructType( [ StructField("enqueuedTime", StringType(), True), StructField("producer", StringType(), True), StructField("recordType", StringType(), True), StructField("schemaVersion", StringType(), True), ] ), True, ), StructField( "data", ArrayType( StructType( [ StructField( "AdditionalData", StructType( [ StructField("condition", StringType(), True), StructField("loadId", StringType(), True), StructField("method", StringType(), True), StructField("note", StringType(), True), StructField("reason", StringType(), True), StructField("reroute", StringType(), True), StructField("resolution", StringType(), True), StructField("review", StringType(), True), StructField("signedBy", StringType(), True), StructField("sortDecision", StringType(), True), StructField("stopSequence", LongType(), True), StructField( "transportType", StringType(), True ), StructField("vehicleId", StringType(), True), ] ), True, ), StructField( "CurrentDestinationFacilityId", StringType(), True ), StructField("CurrentOriginFacilityId", StringType(), True), StructField("CurrentServiceLevel", StringType(), True), StructField("EmployeeId", StringType(), True), StructField("EstimatedArrivalTime", StringType(), True), StructField("EventId", StringType(), True), StructField("EventTimestamp", StringType(), True), StructField("EventType", StringType(), True), StructField("ExceptionCode", StringType(), True), StructField("ExceptionSeverity", StringType(), True), StructField("FacilityId", StringType(), True), StructField("GeneratedAt", StringType(), True), StructField("LocationLatitude", DoubleType(), True), StructField("LocationLongitude", DoubleType(), True), StructField("NextWaypointFacilityId", StringType(), True), StructField("OrganizationId", LongType(), True), StructField( "PlannedPathSnapshot", ArrayType(StringType(), True), True, ), StructField("RelatedExceptionEventId", StringType(), True), StructField("ResolutionAction", StringType(), True), StructField("RouteId", StringType(), True), StructField("ScanDeviceId", StringType(), True), StructField("ScheduleId", StringType(), True), StructField("SequenceNumber", LongType(), True), StructField("ShipmentId", StringType(), True), StructField("SortLaneId", StringType(), True), StructField("SortingEquipmentId", StringType(), True), StructField("TrackingNumber", StringType(), True), ] ), True, ), True, ), ] )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# start here



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Binary to String
# 
# The Kafka `value` column arrives as **binary**. Cast it to a string so we can read the content and next apply JSON parsing:


# CELL ********************

casted_shipment_stream_df = shipment_scan_event_df.withColumn("value", sf.col("value").cast("string"))
display(casted_shipment_stream_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Schematize
# 
# Apply [`from_json`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html) with our defined schema to convert the raw JSON string into a typed **struct** column. Notice how you can now drill into the schema emlements in the cell output (click on a cell and drill through the structure on the right).


# CELL ********************

apply_schema_df = casted_shipment_stream_df.withColumn('value', sf.from_json(sf.col('value'), shipment_scan_event_schema))
display(apply_schema_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Expand First Level Structure
# 
# Use `select('value.*')` to promote the top-level struct fields (`_meta`, `data`) into their own columns:


# CELL ********************

expanded_df = apply_schema_df.select('value.*')
display(expanded_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Explode Array of Records
# 
# The `data` column is an array — [`explode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html) creates one row per element:


# CELL ********************

exploded_df = expanded_df.selectExpr("explode(data) as shipment_scan_event")
display(exploded_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Star Expand All `shipment_scan_event` Columns
# 
# Finally, expand the nested struct so every field becomes a top-level column — giving us a flat, query-ready DataFrame:


# CELL ********************

star_expanded_df = exploded_df.selectExpr("shipment_scan_event.*")
display(star_expanded_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Putting It All Together
# 
# Now that we know how to parse the raw Kafka payload, let's wire up streaming jobs for **bronze** and **silver**:
# 
# | Layer | What it does |
# |-------|--------------|
# | **Bronze** | Light normalization — schema enforcement, `snake_case` columns, audit timestamp |
# | **Silver** | Expand all nested fields into individual top-level columns |


# MARKDOWN ********************

# ### Define Bronze Transforms
# 
# Run the cell below to define helper functions we'll reuse for every source — `snake_case` conversion, column normalization, and an audit timestamp:


# CELL ********************

from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, ArrayType
import re
from typing import Optional


def to_snake_case(name: str) -> str:
    """
    Convert string to snake_case
    
    Handles:
    - CamelCase/PascalCase
    - Punctuation (except underscores)
    - Numbers
    - Preserves existing underscores
    
    Args:
        name: Input string
        
    Returns:
        snake_case string
    """
    # Replace punctuation with space (preserve underscores)
    name = re.sub(r"[^A-Za-z0-9_]+", " ", name)
    # Split camelCase/PascalCase
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name)
    # Add space before numbers
    name = re.sub(r"([a-z])(\d+)", r"\1 \2", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Convert to lowercase and replace spaces with underscores
    result = name.lower().replace(" ", "_")
    # Clean up multiple consecutive underscores
    result = re.sub(r"_+", "_", result)
    
    return result


def normalize_columns_to_snake_case(
    df: DataFrame,
    schema: Optional[StructType] = None
) -> DataFrame:
    """
    Normalize all column names to snake_case recursively
    
    Handles:
    - Top-level columns
    - Nested struct fields (any depth)
    - Arrays of structs
    
    Args:
        df: Input DataFrame
        schema: Optional schema (defaults to df.schema)
        
    Returns:
        DataFrame with all columns renamed to snake_case
    """
    from pyspark.sql import Column as SparkColumn
    
    def build_struct_from_lambda(lambda_var: SparkColumn, struct_type: StructType) -> SparkColumn:
        """Build struct from lambda variable with snake_case field names"""
        return sf.struct(*[
            lambda_var[field.name].alias(to_snake_case(field.name))
            for field in struct_type.fields
        ])
    
    def transform_struct_fields(col_path: str, struct_type: StructType) -> SparkColumn:
        """Recursively transform struct fields to snake_case"""
        return sf.struct(*[
            _transform_field(f"{col_path}.{field.name}", field.name, field.dataType)
            for field in struct_type.fields
        ])
    
    def _transform_field(col_path: str, field_name: str, field_type) -> SparkColumn:
        """Transform a single field based on its type"""
        new_name = to_snake_case(field_name)
        
        if isinstance(field_type, StructType):
            # Nested struct - recursively transform
            return transform_struct_fields(col_path, field_type).alias(new_name)
        elif isinstance(field_type, ArrayType) and isinstance(field_type.elementType, StructType):
            # Array of structs - transform each element
            return sf.transform(
                sf.col(col_path),
                lambda x: build_struct_from_lambda(x, field_type.elementType)
            ).alias(new_name)
        else:
            # Simple field or array of primitives
            return sf.col(col_path).alias(new_name)
    
    if schema is None:
        schema = df.schema
    
    select_exprs = [
        _transform_field(field.name, field.name, field.dataType)
        for field in schema.fields
    ]
    
    return df.select(*select_exprs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write the Bronze Stream
# 
# Now let's wire up the full bronze stream for `shipment_scan_event`. We read from the **Eventstream** (Kafka), apply the bronze transform via [`foreachBatch`](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch), and write to [Delta Lake](https://learn.microsoft.com/fabric/data-engineering/lakehouse-and-delta-tables).
# 
# Execute the cell below:


# CELL ********************

bronze_sse_stream_df = (
    spark.readStream
    .format("kafka")
    .options(**eventstream_options)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

bronze_sse_parsed_df = (
    bronze_sse_stream_df.withColumn("value", sf.col("value").cast("string"))
    .withColumn('value', sf.from_json(sf.col('value'), shipment_scan_event_schema))
    .select('value.*')
    .transform(normalize_columns_to_snake_case, shipment_scan_event_schema)
    .selectExpr("_meta", "explode(data) as data")
    .withColumn('_processing_timestamp', sf.current_timestamp())
)

bronze_sse_stream_query = (bronze_sse_parsed_df.writeStream
    .format('delta')
    .option('checkpointLocation', 'Files/explore/checkpoints/bronze_sse')
    .trigger(availableNow=True)
    .toTable("dbo.bronze_sse")
)

bronze_sse_stream_query.awaitTermination()
print(f'Bronze SSE rows: {spark.table("dbo.bronze_sse").count()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql('SELECT * FROM dbo.bronze_sse LIMIT 5'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 — Build a Bronze → Silver Streaming Pipeline
# 
# The silver layer takes bronze data and applies business logic:
# 
# | Step | What it does |
# |------|--------------|
# | **Flatten** | Expand the `data` struct and promote nested fields into top-level columns |
# | **Audit columns** | Carry `_processing_timestamp` forward from bronze |
# 
# Let's look at the bronze data first — notice the `_meta` struct and the `data` array:


# MARKDOWN ********************

# ### Write the Silver Stream
# 
# Now that you've seen the end-to-end pattern from raw to bronze, define a **transformer function** that encapsulates the generic silver logic — `select('data.*')`, flatten nested structs, and create the audit timestamp.
# 
# Packaging the transform as a function makes it reusable across objects and sets you up for the framework approach in Part 2.
# 
# Wire it up as a stream — reading incrementally from the bronze [Delta table as a streaming source](https://docs.delta.io/latest/delta-streaming.html):


# CELL ********************

def silver_transform(df) -> DataFrame:
    df = df.select('data.*') \
        .withColumn('_processing_timestamp', sf.current_timestamp())
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_sse_stream_df = (
    spark.readStream
    .table('dbo.bronze_sse')
    .transform(silver_transform)
)

silver_sse_stream_query = (silver_sse_stream_df.writeStream
    .format('delta')
    .option('checkpointLocation', 'Files/explore/checkpoints/silver_sse')
    .trigger(availableNow=True)
    .toTable("dbo.silver_sse")
)

silver_sse_stream_query.awaitTermination()
print(f'Bronze SSE rows: {spark.table("dbo.silver_sse").count()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql('SELECT * FROM dbo.silver_sse LIMIT 5'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 🚀 Try It: Re-run the Bronze or Silver Stream
# 
# Go back and **re-execute** the bronze stream (cell above section 6) or the silver stream. Notice what happens:
# 
# - The job completes **much faster**
# - `lastProgress` shows **a smaller number of input rows**
# - No duplicate records were written
# 
# **This is the entire point of stateful streaming.** The checkpoint tracked exactly which data was already processed. If there is nothing new to consume, Spark does nothing. If there's new data, Spark processes it. In a scheduled production pipeline this means each run is only as expensive as the *new* data that arrived since the last run — not a full re-scan of the source. The switch from `read` to `readStream` eliminates an entire class of problems: duplicate records, wasted compute, and the need to build custom incremental state management logic.
# 
# > 💡 Compare this to the batch `customer` write from Section 2 — that job blindly re-appended the same data and produced duplicates. The streaming version with checkpoints avoids that by design.


# MARKDOWN ********************

# ## 🏆 Key Takeaways
# 
# | Concept | What you practiced |
# |---------|--------------------|
# | **`availableNow` trigger** | Batch scheduling with streaming state — process only new data each run |
# | **Checkpoint state** | Re-running a stream skips already-processed files automatically |
# | **`memory` sink** | Iterate on transforms without touching Delta — the safe dev loop |
# | **`foreachBatch`** | Full DataFrame API per micro-batch — write to multiple sinks or apply complex logic |
# | **`lastProgress` / `status`** | Monitor any running or completed stream interactively |
# | **Eventstream (Kafka) pattern** | Same transforms whether the source is files or a message broker |
# | **Bronze pattern** | Schema enforcement + column normalization + audit timestamp |
# | **Silver pattern** | Flatten structs + cleaning + enrichment + audit timestamp |
# 
# ---
# 
# ### 📚 Further Reading
# 
# - [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html)
# - [Delta Lake Table Streaming Reads and Writes](https://docs.delta.io/latest/delta-streaming.html)
# 
# > **Up next → Part 2:** We take the exact same transforms and package them into a reusable streaming framework (ArcFlow) — see how software engineering practices change everything.
# 
# Next open the next **arcflow_framework** Notebook to continue the tutorial.

