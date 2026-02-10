# Sage Re-Auth Procedure (Working Steps)

This is the minimal, working flow to refresh Sage auth without guessing.
Use these steps exactly.

## Preconditions
- Cloud Run service: `invoice-ingest`
- Basic Auth: `BASIC_USER` / `BASIC_PASS`
- Endpoint base URL:
  `https://invoice-ingest-262398202422.europe-west1.run.app`

## 1) Get Sage Authorize URL
Open in browser (will prompt for Basic Auth):
```
https://invoice-ingest-262398202422.europe-west1.run.app/sage/auth-url
```
It returns JSON:
```
{"url":"https://www.sageone.com/oauth2/auth/central?..."}
```
Copy **only** the `url` value.

## 2) Authorize in Sage
Open the `url` in your browser.
After consent you will land on:
```
https://oauth.pstmn.io/v1/browser-callback?code=GB%2F<NEW-CODE>&country=GB&state=123
```
Copy the full callback URL.

## 3) Exchange Code for Refresh Token
Use the code from the callback URL:
```
curl -u "user:tendentious" \
  -H "Content-Type: application/json" \
  -d '{"code":"GB/NEW-CODE"}' \
  "https://invoice-ingest-262398202422.europe-west1.run.app/sage/exchange"
```
Response includes:
```
{"status":"ok","refresh_token":"<LONG_TOKEN>"}
```

## 4) Set Refresh Token in Cloud Run
Cloud Run → `invoice-ingest` → Edit & Deploy New Revision → Variables & Secrets:
```
SAGE_REFRESH_TOKEN = <LONG_TOKEN>
SAGE_ENABLED = 1
SAGE_CLIENT_ID = <CLIENT_ID>
SAGE_CLIENT_SECRET = <CLIENT_SECRET>
```
Deploy.

Never store real credentials in this repo. Use Secret Manager for `SAGE_CLIENT_ID`,
`SAGE_CLIENT_SECRET`, and `SAGE_REFRESH_TOKEN`, and reference them in Cloud Run.

## 5) Verify Refresh Works
```
curl -u "user:tendentious" \
  "https://invoice-ingest-262398202422.europe-west1.run.app/sage/test-refresh"
```
Expected:
```
{"status":"ok"}
```

If you see `DataParsingError`, re-copy the refresh token to ensure there are no hidden
spaces or line breaks.
