import os
import concurrent.futures

from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient, TableEntity
from azure.search.documents import SearchClient
from azure.search.documents.models import IndexingResult

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

def find_index_documents(file_name):
    filter_expr = f"search.ismatch('{file_name}', 'file_name')"
    results = search_client.search(
        search_text="*",
        filter=filter_expr,
        select=["indexKey", "file_path", "file_name"],
    )

    matched_results = []
    for result in results:
        if result["file_name"] == file_name:
            matched_results.append(result)

    return matched_results

def upload_documents(list_index_key: list[str], entity: TableEntity):
    documents = [
        {
            "indexKey": index_key,
            "approve_status": entity.get("approve_status"),
            "hit_id": entity.get("hit_id"),
            "item_id": entity.get("item_id"),
            "file_path": entity.get("file_path"),
            "organization": entity.get("organization"),
            "role": entity.get("role"),
            "site_id": entity.get("site_id"),
            "web_url": entity.get("web_url")
        }
        for index_key in list_index_key
    ]
    return search_client.merge_or_upload_documents(documents=documents)

def process_entity(entity: TableEntity, results: list[IndexingResult]):
    file_name = entity["file_name"]
    docs = find_index_documents(file_name)

    list_index_key = [doc["indexKey"] for doc in docs]
    result = upload_documents(list_index_key, entity)
    results.extend(result)

if __name__ == "__main__":
    client = table_service.get_table_client(TABLE_NAME)

    count_per_page = 100
    results: list[IndexingResult] = []
    try:
        pages = client.list_entities(results_per_page=count_per_page)
        for page in pages.by_page():
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_entity, entity, results) for entity in page]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Exception occurred: {e}")
    finally:
        table_service.close()
