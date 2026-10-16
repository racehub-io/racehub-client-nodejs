import {WebSocketServer} from 'ws';
import {setAuthToken, setUserId, startRecorder, stopRecorder} from '.';
import {
  WS_CONNECTION,
  WS_MESSAGE,
  START_RECORDING,
  STOP_RECORDING,
  FIREBASE_INFO,
} from './actions';

const allowedOrigins = new Set([
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  ...(process.env.RACEHUB_ALLOWED_ORIGINS || '')
    .split(',')
    .map(origin => origin.trim())
    .filter(Boolean),
]);
const wss = new WebSocketServer({
  host: 'localhost',
  port: 8081,
  verifyClient: ({origin, req}) =>
    ['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(
      req.socket.remoteAddress || ''
    ) && (origin === undefined || allowedOrigins.has(origin)),
});

export function sendViaWSS(message: string) {
  // console.log('sending WSS message', message);
  if (wss.clients?.size === 0) {
    return;
  }
  wss.clients.forEach(ws => ws.send(message));
}

interface ParsedBuffer {
  action: string;
  payload: ActionStartRecorderPayload | FirebaseInfo;
}

interface ActionStartRecorderPayload {
  inPort: number;
  outAddress: string;
  outPort: number;
  isForwardingEnabled: boolean;
}

interface FirebaseInfo {
  token: string;
  uid: string;
}

wss.on(WS_CONNECTION, function connection(ws) {
  ws.on(WS_MESSAGE, function message(buffer: Buffer) {
    const parsed: ParsedBuffer = JSON.parse(buffer.toString());
    const {action, payload} = parsed;
    switch (action) {
      case START_RECORDING:
        const {inPort, outAddress, outPort, isForwardingEnabled} =
          payload as ActionStartRecorderPayload;
        startRecorder(inPort, outAddress, outPort, isForwardingEnabled);
        console.log('starts recording');
        break;
      case STOP_RECORDING:
        stopRecorder();
        console.log('stops recording');
        break;
      case FIREBASE_INFO:
        const {token, uid} = payload as FirebaseInfo;
        setAuthToken(token);
        setUserId(uid);
        break;
      default:
        console.log('Unhandled action from rhw');
        break;
    }
  });
});
