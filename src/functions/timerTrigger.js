const { app } = require('@azure/functions');
const { BlobServiceClient } = require('@azure/storage-blob');

const connectionString = process.env.AzureWebJobsStorage;  
const containerName = 'samples-workitems';

const timerTrigger = async function (myTimer, context) {
    const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    const containerClient = blobServiceClient.getContainerClient(containerName);

    for await (const blob of containerClient.listBlobsFlat()) {
      const blobClient = containerClient.getBlobClient(blob.name);
    //   const downloadBlockBlobResponse = await blobClient.download();
    //   const downloadedContent = await streamToString(downloadBlockBlobResponse.readableStreamBody);

      context.log(`Blob name: ${blob.name}`);
    //   context.log(`Blob content: ${downloadedContent}`);

    }

    context.log('Timer trigger function completed:', new Date().toISOString());
}

app.timer('timerTrigger', {
    schedule: '*/10 * * * * *',
    connection: 'AzureWebJobsStorage',
    handler: timerTrigger,
});