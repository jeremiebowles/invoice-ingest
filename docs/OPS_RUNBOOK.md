# Ops Runbook (Invoice Ingest)

Purpose: quick recovery + handoff guide if context is lost. No secrets in this doc.

## System Overview
- **Inbound email**: AWS SES for `invoices@blackcurrants.click`
- **Storage**: S3 bucket `blackcurrants-inbound-raw-472884935082` (prefix `raw/`)
- **Processor**: Lambda `blackcurrants-inbound-handler`
- **API**: Cloud Run service `invoice-ingest` (region `europe-west1`)
- **Main endpoint**: `POST /postmark/inbound` (Basic Auth + requires header `X-Source: ses`)
- **Sage posting**: enabled via env + refresh token secret
- **Idempotency**: Firestore `processed_messages` collection (MessageID key)

## Key Guarantees
- Replaying the same email should **not create duplicates**.
- Postmark is effectively **disabled**: `REQUIRE_SES_SOURCE=1` blocks non‑SES payloads.

## Cloud Run URLs
- Primary: `https://invoice-ingest-262398202422.europe-west1.run.app`
- Google-managed: `https://invoice-ingest-ukiy2ivoaa-ew.a.run.app`

## Health / Version Checks
- `GET /health` → `{"status":"ok"}`
- `GET /version` → shows `revision` + `app_version`

## Critical Env Vars (Cloud Run)
- `BASIC_USER`, `BASIC_PASS`
- `MAX_REQUEST_BYTES` (e.g. `300000`)
- `SAGE_ENABLED=1`
- `SAGE_CLIENT_ID`, `SAGE_CLIENT_SECRET`
- `SAGE_REFRESH_SECRET_NAME` (Secret Manager name)
- `SAGE_BUSINESS_ID`, `SAGE_CONTACT_ID`
- Ledger IDs: `SAGE_LEDGER_5001_ID`, `SAGE_LEDGER_5002_ID`, `SAGE_LEDGER_5004_ID`
- `FIRESTORE_ENABLED=1`
- `FIRESTORE_DATABASE=invoicedb`
- `FIRESTORE_COLLECTION=sage_queue`
- `REQUIRE_SES_SOURCE=1`
- `ALLOWED_FORWARDERS` (comma-separated)
- `RATE_LIMIT_PER_DAY`
- `GUNICORN_CMD_ARGS="--timeout 180 --graceful-timeout 180"`

## Firestore Collections
- `sage_queue`: parsed/posted records
- `processed_messages`: MessageID idempotency
- `rate_limits`: rate limit counters

## AWS SES Setup (Inbound)
- **Domain**: `blackcurrants.click`
- **Receipt rule set**: `blackcurrants-inbound`
- **Rule**: `blackcurrants-inbound-rule`
- **Recipient**: `invoices@blackcurrants.click`
- **Actions**: S3 store + Lambda invoke
- **Lambda**: `blackcurrants-inbound-handler`
- **S3**: `blackcurrants-inbound-raw-472884935082/raw/`

## Lambda Environment
- `S3_BUCKET=blackcurrants-inbound-raw-472884935082`
- `S3_PREFIX=raw/`
- `WEBHOOK_URL=https://invoice-ingest-ukiy2ivoaa-ew.a.run.app/postmark/inbound`
- `BASIC_USER`, `BASIC_PASS`

## Common Failures + Fixes

### 403 Forbidden from Cloud Run
Cause: missing `X-Source: ses` header or wrong Basic Auth.  
Fix: ensure Lambda is sending the header (it does) and env vars are correct.

### 429 Too Many Requests
Cause: rate limiting.  
Fix: raise `RATE_LIMIT_PER_DAY` or disable in Firestore.

### 413 Request Too Large
Cause: `MAX_REQUEST_BYTES` too low.  
Fix: adjust env var.

### Duplicate invoices in Sage
Cause: idempotency not working or Firestore disabled.  
Fix: ensure `FIRESTORE_ENABLED=1`, `processed_messages` collection available.

## Ops Recipes

### Check latest Cloud Run revision
```
gcloud run services describe invoice-ingest --region=europe-west1 \
  --format="value(status.latestReadyRevisionName)"
```

### Force traffic to latest revision
```
gcloud run services update-traffic invoice-ingest --region=europe-west1 --to-latest
```

### Check recent inbound requests
```
gcloud logging read \
  'resource.type=cloud_run_revision resource.labels.service_name=invoice-ingest \
   httpRequest.requestUrl="https://invoice-ingest-262398202422.europe-west1.run.app/postmark/inbound"' \
  --limit=5 --format=json
```

### Check Lambda logs
```
aws logs filter-log-events --region eu-west-2 \
  --log-group-name /aws/lambda/blackcurrants-inbound-handler --limit 20
```

### Check latest raw email in S3
```
aws s3api list-objects-v2 --bucket blackcurrants-inbound-raw-472884935082 \
  --prefix raw/ --query "reverse(sort_by(Contents,&LastModified))[0:3]" --output json
```

### Replay a specific raw email (idempotency test)
```
cat > /tmp/lambda-test-event.json <<'EOF'
{"Records":[{"s3":{"bucket":{"name":"blackcurrants-inbound-raw-472884935082"},"object":{"key":"raw/OBJECT_KEY_HERE"}}}]}
EOF

aws lambda invoke --region eu-west-2 \
  --function-name blackcurrants-inbound-handler \
  --cli-binary-format raw-in-base64-out \
  --payload file:///tmp/lambda-test-event.json /tmp/lambda-invoke-output.json
```

## Sage Refresh Token Rotation (Summary)
1. Get auth URL: `GET /sage/auth-url` (Basic Auth required).
2. Use browser to authorize, capture `code` from redirect.
3. Exchange: `POST /sage/exchange` with JSON `{"code":"..."}`
4. Store new refresh token in Secret Manager and update `SAGE_REFRESH_SECRET_NAME`.

## Duplicate Protection
- **Primary**: `processed_messages` Firestore collection (MessageID).
- **Secondary**: Sage API duplicate search by `vendor_reference` / `reference`.

## Files to Remember
- `main.py` → request handling / parser routing
- `app/sage_client.py` → Sage posting / duplicate search
- `app/firestore_queue.py` → idempotency + queue helpers
- `docs/SAGE_REAUTH.md` → Sage OAuth steps

