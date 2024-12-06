const { app } = require('@azure/functions');
const { BlobServiceClient } = require('@azure/storage-blob');
const DefaultAzureCredential = require('@azure/identity').DefaultAzureCredential;
const fs = require('fs');
const path = require('path');

const defaultAzureCredential = new DefaultAzureCredential();
const accountName = process.env.AZURE_STORAGE_ACCOUNT_NAME;
const containerName = process.env.AZURE_STORAGE_CONTAINER_NAME;

const timerTrigger = async function (myTimer, context) {
  const blobServiceClient = new BlobServiceClient(
    `https://${accountName}.blob.core.windows.net`, 
    defaultAzureCredential
  );
  const containerClient = blobServiceClient.getContainerClient(containerName);

  for await (const blob of containerClient.listBlobsFlat()) {
    const blobClient = containerClient.getBlobClient(blob.name);
    const downloadBlockBlobResponse = await blobClient.download();
    const localFilePath = path.join(__dirname, 'download', blob.name);
    const writeStream = fs.createWriteStream(localFilePath);
    downloadBlockBlobResponse.readableStreamBody.pipe(writeStream);
    context.log(`Blob ${blob.name} has been downloaded to ${localFilePath}`);
    context.log(`Blob name: ${blob.name}`);

    
  }
  
  context.log('Timer trigger function completed:', new Date().toISOString());
}

app.timer('timerTrigger', {
    schedule: '*/10 * * * * *',
    connection: 'AzureWebJobsStorage',
    handler: timerTrigger,
});