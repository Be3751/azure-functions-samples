import os

import azure.functions as func
import azure.durable_functions as df
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient, TableEntity
from dotenv import load_dotenv

from batch_metadata_mapping import find_documents, upload_documents

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")

credential = DefaultAzureCredential()
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
    pages = client.list_entities(results_per_page=count_per_page)
    for page in pages.by_page():
        for tbl_entity in page:
            res = yield context.call_activity("process_entity", tbl_entity)
            results.append(res)
    
    return results

# Activity
@myApp.activity_trigger(input_name="input")
def process_entity(input: dict)->dict:
    tbl_entity = TableEntity(**input)
    file_path = str(tbl_entity["file_path"])
    try:
        docs = find_documents(file_path)
    except Exception as e:
        raise Exception(f"Error finding documents with file_path: {file_path}, original error: {e}") from e

    list_index_key = [doc["indexKey"] for doc in docs]
    try:
        results = upload_documents(list_index_key, tbl_entity)
    except Exception as e:
        raise Exception(f"Error uploading documents with list_index_key: {list_index_key}, original error: {e}") from e
    
    cnt_succeeded = 0
    serialized_results = []
    for res in results:
        serialized_results.append(res.as_dict())
        if res.succeeded:
            cnt_succeeded += 1
                                  
    return {
        "number_of_results": len(serialized_results),
        "number_of_succeeded": cnt_succeeded,
        "number_of_failed": len(serialized_results) - cnt_succeeded,
        "results": serialized_results
    }
