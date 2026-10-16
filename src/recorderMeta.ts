const path = require('path');
const fs = require('fs');
const os = require('os');

import { RecordingMetadata, RecorderMetadata } from './types';

const filesPath = path.join(os.homedir(), 'RaceHub');
const recorderMetadataPath = path.join(filesPath, 'recorderMeta.json');

const readRecorderMetadata = () => {
  if (!fs.existsSync(recorderMetadataPath)) {
    return {};
  }

  const rawRecorderMetadata = fs.readFileSync(recorderMetadataPath);
  return JSON.parse(rawRecorderMetadata);
}

const getRHCFilenames = () => {
  const rhcFilenames = fs.readdirSync(filesPath)
    .filter((file: string) => path.extname(file) === '.rhc');
  return rhcFilenames;
}

export const getRecorderMetadata = () => {
  const recorderMetadata = readRecorderMetadata();

  const rhcFilenames = getRHCFilenames();

  rhcFilenames.forEach((rhcFilename: string) => {
    if (!(rhcFilename in recorderMetadata)) {
      recorderMetadata[rhcFilename] = <RecordingMetadata>{
        filename: rhcFilename,
        parsed: false
      }
    }
  });

  return recorderMetadata;
}

export const setRecorderMetadataParsedComplete = (filename: string, recorderMetadata: RecorderMetadata) => {
  recorderMetadata[filename].parsed = true;
  return recorderMetadata;
}

export const writeRecorderMetadata = (recorderMetadata: RecorderMetadata) => {
  fs.writeFileSync(recorderMetadataPath, JSON.stringify(recorderMetadata));
}
