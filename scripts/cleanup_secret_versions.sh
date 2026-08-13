#!/bin/bash
# One-time cleanup: destroy DISABLED secret versions that accumulated before
# the sage_client.py fix (they were being disabled but never destroyed, and
# DISABLED versions still count as billable "active" versions in Secret Manager).
#
# Usage: ./scripts/cleanup_secret_versions.sh [secret-name]
# If secret-name is omitted, it's read from the Cloud Run service's
# SAGE_REFRESH_SECRET_NAME env var.

set -euo pipefail
export CLOUDSDK_CONFIG=/tmp/gcloud_config
export CLOUDSDK_CORE_PROJECT=project-3b9e6023-203a-4116-a78

SECRET_NAME="${1:-}"

if [ -z "$SECRET_NAME" ]; then
  echo "Looking up SAGE_REFRESH_SECRET_NAME from Cloud Run service..."
  SECRET_NAME=$(gcloud run services describe invoice-ingest \
    --project="$CLOUDSDK_CORE_PROJECT" \
    --region=europe-west1 \
    --format="json(spec.template.spec.containers[0].env)" \
    | python3 -c "
import json, sys
data = json.load(sys.stdin)
env = data['spec']['template']['spec']['containers'][0]['env']
for e in env:
    if e.get('name') == 'SAGE_REFRESH_SECRET_NAME':
        print(e.get('value', ''))
        break
")
fi

if [ -z "$SECRET_NAME" ]; then
  echo "Could not determine secret name. Pass it explicitly:"
  echo "  ./scripts/cleanup_secret_versions.sh projects/PROJECT/secrets/SECRET"
  exit 1
fi

echo "Secret: $SECRET_NAME"
echo "Counting versions by state..."
gcloud secrets versions list "$SECRET_NAME" --format="value(state)" | sort | uniq -c

SECRET_ID="${SECRET_NAME##*/}"

echo ""
echo "Destroying DISABLED versions (this is what's driving the billable count)..."
gcloud secrets versions list "$SECRET_NAME" --filter="state:DISABLED" --format="value(name)" | while read -r vid; do
  echo "Destroying version $vid"
  gcloud secrets versions destroy "$vid" --secret="$SECRET_ID" --project="$CLOUDSDK_CORE_PROJECT" --quiet
done

echo ""
echo "Done. Remaining versions by state:"
gcloud secrets versions list "$SECRET_NAME" --format="value(state)" | sort | uniq -c
