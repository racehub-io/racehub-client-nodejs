# racehub-client-nodejs

Local recording and parsing do not need an account. Connect the web app to `ws://localhost:8081`.

Browser connections accept `http://localhost:3000` and `http://127.0.0.1:3000`.
Set `RACEHUB_ALLOWED_ORIGINS` to add trusted UI origins, separated by commas.
Use the exact scheme, host, and port. Wildcards do not match.
Native clients without an Origin header can connect from loopback.

Cloud upload is optional. Set `RACEHUB_API_BASE_URL` to your own service and sign in through its client. Uploads need a token and user ID.
Uploads are disabled when the service URL is absent.

To package just run `npx pkg .`


Workers
recorder
parser
uploader

Metadata
parserStatus - checks for rhcs
sessionInfo - generated when parsing containing the information of all the generated prqs
uploaderStatus - checks for prq


TODO
Delete rhcs when parsed
Delete parquets when uploaded
Clear entries out for both when they are deleted


02/05/22
- From RHW
UserId
AuthToken
User privilledges for uploading non-player and ai parquets

- To RHW
WS connection to handle firestore document upload for metadata
WS connection to feedback status (recorder/parser/uploader)

## Community use

RaceHub source contributions are available under the scope in [LICENSING.md](LICENSING.md).
Local features do not require a RaceHub subscription.
Third-party games, SDKs, and optional hosting keep their own terms and costs.
See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).
No hosted service or maintenance schedule is included.
