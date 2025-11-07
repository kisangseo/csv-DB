from azure.storage.blob import BlobServiceClient

AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=bcsobcf;"
    "AccountKey=jivFSuIpTOXv30ruihnQB6iE5/p8z2Z0KUihqSsjlYNHjIouD7eIB93bogR9u3t0aGkcqv94EalX+AStQg/yMQ==;"
    "EndpointSuffix=core.windows.net"
)

try:
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    containers = blob_service_client.list_containers()
    print("✅ Connected to Azure Blob!")
    for c in containers:
        print(" -", c["name"])
except Exception as e:
    print("❌ Error:", e)