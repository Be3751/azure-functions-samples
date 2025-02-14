import os

import azure.functions as func
import azure.durable_functions as df
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient
from azure.search.documents import SearchClient
from dotenv import load_dotenv

from batch_metadata_mapping import find_index_documents, upload_documents

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")
SEARCH_SERVICE_NAME = os.getenv("SEARCH_SERVICE_NAME")
SEARCH_INDEX_NAME = os.getenv("SEARCH_INDEX_NAME")

credential = DefaultAzureCredential()
search_client = SearchClient(
    f"https://{SEARCH_SERVICE_NAME}.search.windows.net",
    SEARCH_INDEX_NAME, 
    credential=credential
)
table_service = TableServiceClient(
    endpoint=f"https://{STORAGE_ACCOUNT_NAME}.table.core.windows.net",
    credential=credential
)

# An HTTP-triggered function with a Durable Functions client binding
@myApp.route(route="orchestrators/metadata_mapping")
@myApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    instance_id = await client.start_new("metadata_mapping")
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def metadata_mapping(context):
    count_per_page = 100
    client = table_service.get_table_client(TABLE_NAME)

    results = []
    try:
        pages = client.list_entities(results_per_page=count_per_page)
        for page in pages.by_page():
            res = yield context.call_activity("process_entity", page, results)
            results.append(res)
    finally:
        table_service.close()
    return results

# Activity
@myApp.activity_trigger(input_name="data")
def process_entity(data):
    entity = data["entity"]
    results = data["results"]

    file_name = entity["file_name"]
    docs = find_index_documents(file_name)

    list_index_key = [doc["indexKey"] for doc in docs]
    result = upload_documents(list_index_key, entity)
    results.extend(result)
