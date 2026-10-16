const {parentPort, workerData} = require('worker_threads');
const fetch = require('node-fetch');
const path = require('path');
const fs = require('fs');

const BASEPATH = process.env.RACEHUB_API_BASE_URL;

async function getUploadSignedUrl(filepath, authToken, userId) {
 try {
   const fileName = path.parse(filepath).base

   const requestOptions = {
     method: 'GET',
     headers: {
       'Authorization': `Bearer ${authToken}`
     },
   };

   const result = await fetch(
     `${BASEPATH}/upload/${userId}/${fileName}`,
     requestOptions
   );

   const { url } = await result.json();

   return url;
 } catch (e) {
   console.log(e);
   return '';
 }
}

async function uploadFile(filepath, uploadURL, authToken) {
  console.log(`Uploading ${filepath}`);

  const file = fs.readFileSync(filepath);

  try {
    console.log('Uploading file');
    const headers = new fetch.Headers();
    headers.append('Content-Type', 'application/octet-stream');
    headers.append('Authorization', `Bearer ${authToken}`);

    const requestOptions = {
      method: 'PUT',
      headers,
      body: file,
      redirect: 'follow',
    };
    const result = await fetch(uploadURL, requestOptions);

    console.log('Upload succeeded? ', result.ok ? 'yes' : 'no');

    return result.ok;
  } catch (e) {
    console.log(e);
    return false;
  }
}

async function start() {
  if (!BASEPATH) {
    parentPort.postMessage({success: false, type: 'end'});
    return;
  }
  const {filepath, authToken, userId} = workerData;
  let success = false;

  try {
    const signedUrl = await getUploadSignedUrl(filepath, authToken, userId);
    const result = await uploadFile(filepath, signedUrl, authToken);
    success = true
  } catch (e) {
    console.log(e);
  }

  parentPort.postMessage({
    success,
    type: 'end',
  });
}

start();
