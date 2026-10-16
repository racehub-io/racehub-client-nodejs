const path = require('path');
const fs = require('fs');
const os = require('os');

import { Upload } from './types';

const filesPath = path.join(os.homedir(), 'RaceHub');

const sessionsListPath = path.join(filesPath, 'sessionsList.json');

let uploadList: Upload[] = [];

export const readSessionMetadata = (uniqueSessionId: string) => {
  const sessionMetadataPath = path.join(filesPath, `session-${uniqueSessionId}.json`);

  if (!fs.existsSync(sessionMetadataPath)) {
    return {};
  }

  const rawSessionMetadata = fs.readFileSync(sessionMetadataPath);
  return JSON.parse(rawSessionMetadata);
}

export const setUploadComplete = (uniqueSessionId: string, participantId: string) => {
  const sessionMetadata = readSessionMetadata(uniqueSessionId);
  sessionMetadata.participants[participantId].uploaded = true;
  const sessionMetadataPath = path.join(filesPath, `session-${uniqueSessionId}.json`);
  fs.writeFileSync(sessionMetadataPath, JSON.stringify(sessionMetadata));
}

export const readSessionsList = () => {
  if (!fs.existsSync(sessionsListPath)) {
    return {};
  } else {
    const rawSessionsList = fs.readFileSync(sessionsListPath);
    return JSON.parse(rawSessionsList);
  }
}

const updateUploadList = () => {
  uploadList = [];
  const sessionsList = readSessionsList();

  const rhcFilepaths = Object.keys(sessionsList);
  rhcFilepaths.forEach((rhcFilepath: string) => {
    const uniqueSessionIds = sessionsList[rhcFilepath]
    uniqueSessionIds.forEach((uniqueSessionId: string) => {
      const sessionMetadata = readSessionMetadata(uniqueSessionId);
      // check if sessionMetadata is empty
      if (Object.keys(sessionMetadata).length === 0) {
        return;
      }
      const participantIds = Object.keys(sessionMetadata.participants);
      participantIds.forEach((participantId: string) => {
        const participant = sessionMetadata.participants[participantId];
        if (participant.isAi || !participant.isPlayer) {
          return;
        }
        const { uploaded, filepath } = participant;
        if (!uploaded) {
          uploadList.push({filepath});
        }
      })
    })
  })

  // filter update list on files that exist
  uploadList = uploadList.filter(upload => fs.existsSync(upload.filepath));
}

export const getNextUpload = () => {
  if (uploadList.length === 0) {
    updateUploadList();
  }

  if (uploadList.length) {
    const nextUpload = uploadList.pop();
    if (nextUpload) {
      return nextUpload
    } else {
      return {filepath: ''}
    }
  }

  return {filepath: ''}
}
