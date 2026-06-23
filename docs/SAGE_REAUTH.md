# Sage Re-Auth Procedure (Working Steps)

This is the minimal, working flow to refresh Sage auth without guessing.
Use these steps exactly.

## Preconditions
- Cloud Run service: `invoice-ingest`
- Basic Auth: `BASIC_USER` / `BASIC_PASS`
- Endpoint base URL (must use this exact format — callback URI is derived from the request host):
  `https://invoice-ingest-262398202422.europe-west1.run.app`
- You must be logged into Sage in your browser before opening the auth URL

## Registered Sage Callback URI
```
https://invoice-ingest-262398202422.europe-west1.run.app/sage/callback
```
This is the only registered redirect URI in the Sage developer portal. Do NOT use
the Postman URI, localhost, or any other URL — they will all return "Authorise Application Error".

## 1) Get Sage Authorize URL
Run (will prompt for Basic Auth):
```bash
curl -u "BASIC_USER:BASIC_PASS" \
  "https://invoice-ingest-262398202422.europe-west1.run.app/sage/auth-url"
```
It returns JSON with a `url` field. Copy **only** the `url` value.

Note: Do NOT add `?use_callback=false` — that switches to the Postman redirect URI
which is not registered and will fail.

## 2) Authorize in Sage
Open the `url` in your browser (logged into Sage).
After consent, Sage redirects to the Cloud Run `/sage/callback` endpoint which
automatically exchanges the code and stores the refresh token in Secret Manager.
You will see a success page in the browser.

No manual code extraction or `/sage/exchange` call needed.

## 3) Verify Refresh Works
```bash
curl -u "BASIC_USER:BASIC_PASS" \
  "https://invoice-ingest-262398202422.europe-west1.run.app/sage/test-refresh"
```
Expected:
```
{"status":"ok"}
```

## Troubleshooting

**"Authorise Application Error" from Sage:**
- Make sure you are using the `262398202422.europe-west1.run.app` URL (not the
  `ukiy2ivoaa-ew.a.run.app` hash URL) when calling `/sage/auth-url` — the
  callback URI is built from the request host and must match the registered URI exactly.
- Make sure you are logged into Sage in the browser before opening the URL.
- Do NOT use `?use_callback=false` (Postman URI not registered).
- Do NOT use the `sage_to_secretmanager.py` script (localhost URI not registered — see below).

**`DataParsingError` on test-refresh:**
Re-run the auth flow — the token may have been stored with hidden whitespace.

Never store real credentials in this repo. Secret Manager holds the refresh token.

## sage_to_secretmanager.py — What It Does and Why It Must Not Be Used

`sage_to_secretmanager.py` is a local developer utility script that was written to
eliminate copy-paste corruption when rotating the Sage refresh token. It:

1. Spins up a temporary HTTP server on `localhost:8080` as the OAuth redirect target.
2. Opens a browser to the Sage OAuth authorisation page.
3. Catches the `?code=` callback, exchanges it for tokens via
   `https://oauth.accounting.sage.com/token`.
4. Pipes the refresh token directly into Secret Manager via
   `gcloud secrets versions add --data-file=-` (stdin), avoiding any clipboard
   line-break corruption.

**It does not work and must not be used or deployed, for two reasons:**

1. **Redirect URI not registered.** Sage only accepts
   `https://invoice-ingest-262398202422.europe-west1.run.app/sage/callback` as a
   redirect URI. The script uses `http://localhost:8080/callback`, which Sage
   rejects with "Authorise Application Error" before the exchange can happen.

2. **Hardcoded credentials.** The `CLIENT_ID` and `CLIENT_SECRET` are written
   in plain text in the file. Deploying or committing this file to any shared or
   public location would expose those credentials.

The script should remain in the repo as a local reference only (it is untracked
by git). Use the Cloud Run `/sage/auth-url` → browser → automatic callback flow
described above instead.
