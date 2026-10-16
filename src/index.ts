import {Worker} from 'worker_threads';
import {WORKER_MESSAGE, WORKER_ERROR} from './actions';
import {
  getRecorderMetadata,
  writeRecorderMetadata,
  setRecorderMetadataParsedComplete,
} from './recorderMeta';
import {
  readSessionMetadata,
  getNextUpload,
  setUploadComplete,
} from './uploaderMeta';
import {RecordingMetadata, RecorderMetadata, Upload} from './types';
import {version} from '../package.json';
import {sendViaWSS} from './server';
import './server';

const path = require('path');
const fs = require('fs');
const os = require('os');

let recorderMetadata: RecorderMetadata = {};
let recorderWorker: Worker | null = null;
let parserWorker: Worker | null = null;
let uploaderWorker: Worker | null = null;
let recorderFilename: string = '';

let authToken = '';
let userId = '';

export function setAuthToken(_authToken: string) {
  authToken = _authToken;
}
export function setUserId(_userId: string) {
  userId = _userId;
}

// PARSER INTERVAL
setInterval(() => {
  if (parserWorker) {
    console.log('theres a parser running so returning');
    return;
  }

  recorderMetadata = getRecorderMetadata();
  writeRecorderMetadata(recorderMetadata);
  const sessions: RecordingMetadata[] = Object.values(recorderMetadata);
  const sessionsNotUploaded = sessions.filter(session => !session.parsed);

  // No sessions to parse! :)
  if (sessionsNotUploaded.length === 0) {
    console.log('no sessions to parse');
    return;
  }

  // Start to parse the first unparsed file
  for (let session of sessionsNotUploaded) {
    const filename: string = session.filename;
    if (filename !== recorderFilename) {
      startParser(filename);
      break;
    }
  }
}, 5e3);

const checkUpload = () => {
  if (uploaderWorker) {
    console.log('theres an uploader running so returning');
    setTimeout(checkUpload, 10e3);
    return;
  }
  
  if (!process.env.RACEHUB_API_BASE_URL || authToken === '' || userId === '') {
    console.log('upload service or authentication is absent');
    setTimeout(checkUpload, 5e3);
    return;
  }

  const nextUpload: Upload = getNextUpload();

  const {filepath} = nextUpload;

  if (filepath) {
    console.log('Uploading ', filepath);
    startUploader(filepath);
  } else {
    console.log('no parquets to upload');
    setTimeout(checkUpload, 10e3);
  }
};

export const startRecorder = async (
  inPort: number = 30501,
  outAddress: string = '127.0.0.1',
  outPort: number = 30500,
  isForwardingEnabled: boolean = false
) => {
  if (recorderWorker) {
    return;
  }

  recorderWorker = new Worker(`${__dirname}/workers/recorder.js`, {
    workerData: {
      inPort,
      outAddress,
      outPort,
      isForwardingEnabled,
      version,
    },
  });

  recorderWorker.on(WORKER_MESSAGE, (result: any) => {
    if (result.type === 'status') {
      const { lastMessage, messageCount, sessionTime, sessionType, track } = result;
      const message = {
        action: 'RECORDER_STATUS',
        payload: { lastMessage, messageCount, sessionTime, sessionType, track },
      };
      sendViaWSS(JSON.stringify(message));
      process.stdout.write('.');
    } else if (result.type === 'file') {
      recorderFilename = result.fileName;
    } else if (result.type === 'end' && recorderWorker) {
      recorderWorker.terminate();
      recorderWorker = null;
      console.log('Recording session terminated');
    }
  });

  recorderWorker.on(WORKER_ERROR, e => {
    console.error(WORKER_ERROR, e);
  });
};

export const stopRecorder = () => {
  if (!recorderWorker) {
    return;
  }
  recorderWorker.postMessage('close');
};

const startParser = (filename: string) => {
  if (parserWorker) {
    return;
  }

  if (!filename) {
    return;
  }

  parserWorker = new Worker(`${__dirname}/workers/parser.js`, {
    workerData: {
      filename,
    },
  });

  parserWorker.on(WORKER_MESSAGE, (result: any) => {
    if (result.type === 'end' && parserWorker) {
      parserWorker.terminate();
      parserWorker = null;
      console.log('Parser terminated');
      recorderMetadata = setRecorderMetadataParsedComplete(
        filename,
        recorderMetadata
      );
      writeRecorderMetadata(recorderMetadata);
    } else {
      console.log(result);
    }
  });

  parserWorker.on(WORKER_ERROR, e => {
    console.error(WORKER_ERROR, e);
  });
};

const createParquetMetadata = (filepath: string) => {
  const filename = path.parse(filepath).base;

  const [uniqueSessionId, participantId_with_ext] = filename.split('-');
  const participantId = participantId_with_ext.split('.')[0];

  const sessionMetadata = readSessionMetadata(uniqueSessionId);

  const participantMetadata = sessionMetadata.participants[participantId];

  const session = {
    userId,
    uniqueSessionId: uniqueSessionId,
    sessionId: sessionMetadata.sessionId,
    filename: sessionMetadata.filename,
    fileCreated: sessionMetadata.fileCreated,
    fileModified: sessionMetadata.fileModified,
    trackName: sessionMetadata.trackName,
    trackLength: sessionMetadata.trackLength,
    sessionType: sessionMetadata.sessionType,
    totalLaps: sessionMetadata.totalLaps,
    trackTemperature: sessionMetadata.trackTemperature,
    airTemperature: sessionMetadata.airTemperature,
    weather: sessionMetadata.weather,
    rowCount: sessionMetadata.rowCount,
  };

  const participant = {
    uniqueSessionId: uniqueSessionId,
    sessionId: sessionMetadata.sessionId,
    participantId,
    lapIds: participantMetadata.lapIds,
    isPlayer: participantMetadata.isPlayer,
    isAi: participantMetadata.isAi,
    firstName: participantMetadata.firstName,
    lastName: participantMetadata.lastName,
    abbreviation: participantMetadata.abbreviation,
    nationality: participantMetadata.nationality,
    name: participantMetadata.name,
    teamName: participantMetadata.teamName,
    raceNumber: participantMetadata.raceNumber,
  };

  const laps = participantMetadata.lapIds.map((lapId: string) => {
    return sessionMetadata.laps[lapId];
  });

  const parquetMetadata = {
    session,
    participant,
    laps,
  };

  return parquetMetadata;
};

export const startUploader = (filepath: string) => {
  if (uploaderWorker) {
    return;
  }

  if (!filepath) {
    return;
  }

  const parquetMetadata = createParquetMetadata(filepath);
  const {session, participant} = parquetMetadata;
  const {uniqueSessionId} = session;
  const {participantId} = participant;

  uploaderWorker = new Worker(`${__dirname}/workers/uploader.js`, {
    workerData: {
      filepath,
      authToken,
      userId,
      parquetMetadata,
    },
  });

  uploaderWorker.on(WORKER_MESSAGE, (result: any) => {
    if (result.type === 'end' && uploaderWorker) {
      const { success } = result;
      if (success) {
        uploaderWorker.terminate();
        uploaderWorker = null;
        console.log('Uploader terminated');
        setUploadComplete(uniqueSessionId, participantId);
        const message = {
          action: 'WRITE_PARQUET_META',
          payload: parquetMetadata,
        };
        sendViaWSS(JSON.stringify(message));
      } else {
        console.log('Uploader failed');
      }
      checkUpload();
    } else {
      console.log('message', result);
    }
  });

  uploaderWorker.on(WORKER_ERROR, e => {
    console.error(WORKER_ERROR, e);
  });
};

checkUpload();
