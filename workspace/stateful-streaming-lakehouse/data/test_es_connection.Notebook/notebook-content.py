# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

eventstream = "shipment_scan_events"
eventstream_source_name = "PackageScanners"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eventstreams = fabric_rest.get(path_or_url=f"/v1/workspaces/{workspace_id}/eventstreams")
eventstream_id = [item['id'] for item in eventstream_response.json()['value'] if item['displayName'] == eventstream][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

destination_connection['accessKeys']['primaryConnectionString']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
