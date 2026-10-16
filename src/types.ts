export interface RecordingMetadata {
  filename: string;
  parsed: boolean;
}

export interface RecorderMetadata {
  [key: string]: RecordingMetadata;
}

export interface Upload {
  filepath: string;
}
