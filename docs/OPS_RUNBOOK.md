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

## Deploying Code Changes

### Standard deploy procedure

Always deploy with `--no-traffic` first, verify, then cut over.

```bash
# 1. Build and create new revision (no traffic yet)
gcloud run deploy invoice-ingest \
  --source . \
  --region europe-west1 \
  --no-traffic

# 2. Check the new revision started cleanly
gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=invoice-ingest' \
  --limit=20 --format=json

# 3. Verify health and Sage connectivity
curl -u user:PASSWORD "https://invoice-ingest-262398202422.europe-west1.run.app/health"
curl -u user:PASSWORD "https://invoice-ingest-262398202422.europe-west1.run.app/sage/test-refresh"

# 4. Cut traffic over (replace revision name with actual)
gcloud run services update-traffic invoice-ingest \
  --region=europe-west1 \
  --to-revisions=invoice-ingest-XXXXX-xxx=100
```

### Retrieving Basic Auth credentials
BASIC_USER and BASIC_PASS live in the Cloud Run env vars — no need to look them up elsewhere:
```bash
gcloud run services describe invoice-ingest --region=europe-west1 --format=json \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
envs = {e['name']: e.get('value','') for e in data['spec']['template']['spec']['containers'][0].get('env', []) if 'value' in e}
print('BASIC_USER:', envs.get('BASIC_USER'))
print('BASIC_PASS:', envs.get('BASIC_PASS'))
"
```

### Rolling back
```bash
# List recent revisions
gcloud run revisions list --service=invoice-ingest --region=europe-west1 --format="value(name)" | head -5

# Route traffic back to a known-good revision
gcloud run services update-traffic invoice-ingest \
  --region=europe-west1 \
  --to-revisions=invoice-ingest-XXXXX-xxx=100
```

### Adding a new supplier parser
1. Create `app/parsers/<supplier>.py` with a `parse_<supplier>(text: str) -> InvoiceData` function.
2. Look up the Sage contact ID:
   ```bash
   curl -u user:PASSWORD \
     "https://invoice-ingest-262398202422.europe-west1.run.app/sage/contacts/search?q=Supplier+Name"
   ```
   Use the returned `id` as `contact_id` in the parser.
3. Add `_text_looks_like_<supplier>()` detection function to `main.py`.
4. Add the import and `elif` branch to `_detect_and_parse()` in `main.py`.
5. Deploy using the standard procedure above.

### Python / buildpack compatibility (important)
The Cloud Build buildpack (`google-24-full`) automatically picks the latest Python version.
As of June 2026 this is **Python 3.14**, which breaks `google-cloud-firestore` /
`google-cloud-secret-manager` at startup with:
```
TypeError: Metaclasses with custom tp_new are not supported.
```
The service crash-loops silently — no requests are served and no Firestore records are written.

**Fix applied (June 2026):** `.python-version` pins the runtime to `3.13`. This is confirmed
working. The buildpack available versions on `ubuntu2404` are `3.13.x` and `3.14.x` only —
**3.12 is not available** and will cause a build failure if specified.

If a future deployment crashes with this error, check `.python-version` is still present
and set to a 3.13.x-compatible value. If the buildpack drops 3.13 support and forces 3.14+,
the alternative is to bump the Google Cloud package versions in `requirements.txt` until
a protobuf release adds Python 3.14 compatibility.

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

