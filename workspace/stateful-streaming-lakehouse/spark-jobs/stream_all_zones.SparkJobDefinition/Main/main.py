"""
ArcFlow Spark Job Definition - Main Entry Point

This file is the entry point for Microsoft Fabric Spark Job Definition.

DEPLOYMENT INSTRUCTIONS:
========================

1. Build the wheel:
   $ uv build
   
2. In Fabric Workspace:
   - Go to Environment settings
   - Upload: dist/arcflow-0.1.6-py3-none-any.whl
   - Add to environment libraries
   
3. Create Spark Job Definition:
   - Main file: Upload this file (main.py)
   - Reference files: Upload pipeline_config.py
   - Environment: Select environment with arcflow wheel
   - Reference Lakehouse: Select the Lakehouse where data should be written
   
4. Configure (in pipeline_config.py):
   - Define your tables
   - Define dimensions
   - Register DataFrame transformation functions with the `@register_zone_transfomer` decorator
   
5. Run the Spark Job Definition

CONFIGURATION:
==============
- Edit pipeline_config.py to define tables and transformations
- All default paths are in arcflow.config.ArcFlowDefaults
- Override config below in get_pipeline_config() as needed

"""
from arcflow import Controller
from lakegen.generators.mcmillan_industrial_group import McMillanDataGen
import notebookutils
from pyspark.sql import SparkSession
from pipeline_config import tables
import argparse
from contextlib import contextmanager

import logging
import sys
import time

def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-for-n-minutes", type=float,
                        help="Stop the pipeline after N minutes (default: None).")
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args(argv)

def configure_logging(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # Only show INFO for arcflow/lakegen, silence everything else
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger("arcflow").setLevel(logging.INFO)
    logging.getLogger("lakegen").setLevel(logging.INFO)
    logging.getLogger("__main__").setLevel(logging.INFO)
    return logging.getLogger(__name__)

def create_spark(app_name, debug):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.native.enabled", False)
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config('spark.databricks.delta.autoCompact.enabled', True)
        .config('spark.microsoft.delta.targetFileSize.adaptive.enabled', True)
        .config('spark.databricks.delta.autoCompact.onCheckpointOnly.enabled', True)
        .config('spark.microsoft.delta.optimize.fileLevelTarget.enabled', True)
        .config('spark.microsoft.delta.snapshot.driverMode.enabled', True)
        .config('spark.databricks.delta.properties.defaults.enableDeletionVectors', True)
        .config('spark.databricks.delta.optimizeWrite.enabled', True) # OW enabled since it's streaming micro batches
        .config('spark.ui.retainedJobs', '200')
        .config('spark.ui.retainedStages', '200')
        .config('spark.ui.retainedTasks', '5000')
        .config('spark.sql.streaming.ui.retainedQueries', '100')
        .config('spark.sql.ui.retainedExecutions', '200')
        .config('spark.sql.shuffle.partitions', 4) # set low to prevent over shuffling for small streaming jobs and maximize multi-query parallelism
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO" if debug else "ERROR")
    return spark

def get_conenction_strings():
    import sempy.fabric as fabric

    workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
    eventstream = "shipment_scan_events"
    source_name = "PackageScanners"
    destination_name = "Spark"

    fabric_rest = fabric.FabricRestClient()

    eventstreams = []
    url = f"/v1/workspaces/{workspace_id}/eventstreams"
    while url:
        response = fabric_rest.get(path_or_url=url).json()
        eventstreams += response["value"]
        url = response.get("continuationUri")

    eventstream_id = [item["id"] for item in eventstreams if item["displayName"] == eventstream][0]

    base_url = f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}"
    topology = fabric_rest.get(path_or_url=f"{base_url}/topology").json()

    source_id = [item["id"] for item in topology["sources"] if item["name"] == source_name][0]
    destination_id = [item["id"] for item in topology["destinations"] if item["name"] == destination_name][0]

    source_connection = fabric_rest.get(path_or_url=f"{base_url}/sources/{source_id}/connection").json()
    destination_connection = fabric_rest.get(path_or_url=f"{base_url}/destinations/{destination_id}/connection").json()

    return source_connection['accessKeys']['primaryConnectionString'], destination_connection['accessKeys']['primaryConnectionString']



@contextmanager
def job_lock(lock_uri: str):
    import json
    import threading
    from datetime import datetime, timezone

    import fsspec
    from fsspec.asyn import sync
    fs, path = fsspec.core.url_to_fs(lock_uri)

    try:
        fs.pipe_file(path, b"{}", overwrite=False)
    except Exception as exc:
        if exc.__class__.__name__ not in {"FileExistsError", "ResourceExistsError"}:
            raise

    container, blob, _ = fs.split_path(path)
    blob_client = fs.service_client.get_blob_client(container=container, blob=blob)

    try:
        lease = sync(fs.loop, blob_client.acquire_lease, lease_duration=60)
    except Exception as exc:
        if getattr(exc, "error_code", None) == "LeaseAlreadyPresent":
            raise RuntimeError(f"Job lock is already held: {lock_uri}") from exc
        raise

    marker = {
        "acquired_at": datetime.now(timezone.utc).isoformat(),
    }
    sync(
        fs.loop,
        blob_client.upload_blob,
        json.dumps(marker).encode(),
        overwrite=True,
        lease=lease,
    )

    stop = threading.Event()
    renewal_error = []

    def renew():
        while not stop.wait(30):
            try:
                sync(fs.loop, lease.renew)
            except Exception as exc:
                renewal_error.append(exc)
                stop.set()
                return

    thread = threading.Thread(target=renew, daemon=True)
    thread.start()

    try:
        yield
        if renewal_error:
            raise RuntimeError(f"Lease renewal failed: {lock_uri}") from renewal_error[0]
    finally:
        stop.set()
        thread.join()
        try:
            sync(fs.loop, lease.release)
        except Exception:
            if not renewal_error:
                raise

def main(argv):
    args = parse_args(argv)
    logger = configure_logging(args.debug)
    spark = create_spark("streamShipmentsApp", args.debug)

    default_workspace_id = notebookutils.runtime.context['currentWorkspaceId']
    default_lakehouse_id = notebookutils.runtime.context['defaultLakehouseId']
    onelake_endpoint = spark.sparkContext._jsc.hadoopConfiguration().get("trident.onelake.endpoint").split('//')[1]
    lakehouse_root_uri=f"abfss://{default_workspace_id}@{onelake_endpoint}/{default_lakehouse_id}"

    logger.info(lakehouse_root_uri)

    job_lock_uri = f"{lakehouse_root_uri}/Files/job_locks/stateful_streaming_lakehouse.lock"

    with job_lock(job_lock_uri):
        logger.info("=" * 80)
        logger.info("Getting eventstream connection strings")
        logger.info("=" * 80)

        producer_connection_string, consumer_connection_string = get_conenction_strings()

        logger.info("=" * 80)
        logger.info("Starting LakeGen: McMillanDataGen")
        logger.info("=" * 80)

        data_gen = McMillanDataGen(
            target_folder_uri=f"{lakehouse_root_uri}/Files/landing/",
            kafka_connection_string=producer_connection_string,
            output_type_map={
                "order": "json",
                "shipment": "json",
                "shipment_scan_event": "kafka",
                "item": "parquet",
                "route": "parquet",
                "servicelevel": "parquet",
                "facility": "parquet",
                "exceptiontype": "parquet",
                "customer": "parquet",
            },
            max_events_per_second=1000,
            concurrenct_threads=1
        )
        data_gen.start(verbose=False)

        logger.info("=" * 80)
        logger.info("Starting ArcFlow ELT Framework")
        logger.info("=" * 80)

        # Configure pipeline
        config = {
            'streaming_enabled': True,
            'checkpoint_uri': "Files/checkpoints",
            'archive_uri': "Files/archive",
            'landing_uri': "Files/landing",
            'trigger_interval': '2 seconds', # default if not set at table level
            'event_driven_chaining': True, # if True, downstream transformations will be triggered immediately after upstream completes instead of waiting for next trigger interval
            'await_termination': True, # await_termination needed to keep Spark job from reaching terminal state
            'job_lock_timeout_seconds': 60, # Timeout for acquiring job lock to prevent multiple concurrent runs of the same job
            'job_lock_path': f"{lakehouse_root_uri}/Files/job_locks", # abfss path for job locks because it's not written via spark
	    	'job_lock_enabled': False, # Disable job lock since a job lock is already aquired to wrap the data generator
	    	'job_id': 'stateful_streaming_lakehouse'
        }

        # Step 2: Initialize controller
        logger.info("Initializing ArcFlow Controller...")
        tables["shipment_scan_event"].source_uri = consumer_connection_string
        controller = Controller(
            spark=spark,
            config=config,
            table_registry=tables
        )
        logger.info("✓ Controller initialized")

        # Step 3: Run full pipeline
        logger.info("Starting full ELT pipeline...")
        controller.run_full_pipeline(zones=['bronze', 'silver'])

        if args.run_for_n_minutes:
            # Pipeline is non-blocking — wait for the requested duration, then stop gracefully
            run_seconds = 60 * args.run_for_n_minutes
            logger.info(f"Pipeline will run for {run_seconds / 60} minutes ({int(run_seconds)}s)")

            deadline = time.time() + run_seconds
            try:
                while time.time() < deadline:
                    remaining = deadline - time.time()
                    time.sleep(min(remaining, 30))
                    elapsed = int(time.time() + run_seconds - deadline)
                    logger.info(
                        f"  [{elapsed // 60}m {elapsed % 60:02d}s / "
                        f"{int(run_seconds // 60)}m elapsed]"
                    )
            except KeyboardInterrupt:
                logger.info("Interrupted — stopping early")

            logger.info("Time limit reached — stopping data generator and streams")
            data_gen.stop()
            controller.stop_all()
            logger.info("Pipeline finished successfully")

if __name__ == "__main__":
    main(sys.argv[1:])