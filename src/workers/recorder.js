/*
 * recorder.js
 * This worker records telemetry from the game and saves it into an .rhc file
 */

const path = require('path');
const fs = require('fs');
const os = require('os');
const zlib = require('zlib');
const dgram = require('dgram');
const {parentPort, workerData} = require('worker_threads');
const {F1TelemetryClient, constants} = require('@racehub-io/f1-telemetry-client');

const {parsePacketHeader, parseBufferMessage} = F1TelemetryClient;

const {SESSION_TYPES, TRACKS} = constants;

////// Constructor //////
let file,
  socket,
  isRunning,
  sessionUID,
  lastMessage,
  messageCount = 0;

const verbose = true;
const DEFAULT_RECEIVE_PORT = 20777;

const {
  outPort,
  outAddress,
  version,
  isForwardingEnabled = false,
  inPort = DEFAULT_RECEIVE_PORT,
} = workerData;

// const
//   outPort = 30500,
//   outAddress = '',
//   version = '0.0.0',
//   isForwardingEnabled = false,
//   inPort = 30501;

parentPort.on('message', message => {
  if (message === 'close') {
    stop();
  }
});

////// Update Recorder Info //////
function updateRecorderInfo(info) {
  parentPort.postMessage({
    type: 'status',
    ...info
  });
}

////// Start function //////
function start() {
  if (isRunning) {
    return;
  }

  socket = dgram.createSocket('udp4');

  socket.on('listening', () => {
    if (!socket) {
      return;
    }
    const address = socket.address();
    if (verbose) {
      console.log(`UDP socket listening on ${address.address}:${address.port}`);
      if (isForwardingEnabled) {
        console.log(`Forwarding to ${outAddress}:${outPort}`);
      }
    }
    socket.setBroadcast(true);
  });

  socket.on('message', m => {
    parseMessage(m);
  });

  socket.bind(inPort);
  isRunning = true;
}

////// Stop function //////

function stop() {
  if (!isRunning || !socket) {
    return;
  }

  socket.close(() => {
    if (verbose) {
      console.log('UDP socket closed');
    }

    socket = undefined;

    if (!file) {
      parentPort.postMessage({
        type: 'end',
      });
      return;
    }

    file.end();

    file.on('finish', () => {
      console.log('File finished');

      // this is here due to an odd race condition that doesn't correctly close the file
      // with the timeout we wait for the file to be ended and then we successfully tell the main process
      // so that it can continue with the parsing of the file
      setTimeout(() => {
        console.log('Ending worker');
        parentPort.postMessage({
          type: 'end',
        });
        file = undefined;
      }, 5e3);
    });

    isRunning = false;
  });
}

////// Parse Message //////
function parseMessage(m) {
  const header = parsePacketHeader(m, true);
  const {m_packetFormat, m_sessionUID} = header;

  if (!file) {
    createArchive(m_packetFormat, m_sessionUID);
  }
  else if (sessionUID != m_sessionUID) {
    createArchive(m_packetFormat, m_sessionUID);
  }

  // TODO: in the future it's likely we wont want to record m_sessionUID === 0 as this is the menu screens.

  sessionUID = m_sessionUID;

  messageCount++;
  lastMessage = new Date().getTime();

  if (socket && isForwardingEnabled) {
    socket.send(m, 0, m.length, outPort, outAddress);
  }
  if (file) {
    file.write(m);
  }

  if (header.m_packetId === 1) {
    msg = parseBufferMessage(m);
    const info = {
      messageCount,
      lastMessage,
      sessionTime: msg.packetData.data.m_header.m_sessionTime,
      sessionType: SESSION_TYPES[msg.packetData.data.m_sessionType].long,
      track: TRACKS[msg.packetData.data.m_trackId].name,
    };
    updateRecorderInfo(info);
  }
}

function createArchive(m_packetFormat, m_sessionUID) {
  if (file) {
    file.end();
  }
  file = zlib.createGzip();
  file.pipe(createStream(m_packetFormat, m_sessionUID));

  const [major, minor, patch] = version.split('.');
  // TODO: Add timestamp and likely userID as well
  const headerRHCVersion = new Uint16Array([major, minor, patch]);
  const headerRecorderType = new Uint16Array([m_packetFormat]);
  const headerRHCVersionBuffer = Buffer.from(headerRHCVersion.buffer);
  const headerRecorderTypeBuffer = Buffer.from(headerRecorderType.buffer);
  const headerBuffer = Buffer.concat([
    headerRHCVersionBuffer,
    headerRecorderTypeBuffer,
  ]);
  file.write(headerBuffer);
}

////// Create Stream //////
function createStream(packetFormat, sessionId) {
  const basePath = path.join(os.homedir(), 'RaceHub');
  const fileName = `${Date.now()}_${packetFormat}_${sessionId}.rhc`;
  const filePath = path.join(
    basePath,
    fileName,
  );
  console.log(`Recording to ${filePath}`);
  parentPort.postMessage({
    type: 'file',
    filePath,
    fileName,
    packetFormat,
    sessionId,
  });
  fs.mkdirSync(basePath, {
    recursive: true,
  });
  return fs.createWriteStream(filePath);
}

////// Start Function //////
start();
