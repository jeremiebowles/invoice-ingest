# Firestore Queue Notes

When `FIRESTORE_ENABLED=1`, each inbound email is persisted to Firestore
before posting to Sage. Records are updated with status:
- `parsed` (stored before posting)
- `posted` (Sage created an id)
- `skipped` (duplicate)
- `error` (Sage error)
- `queued` (Sage disabled)
- `no_pdf` (no PDF found)

## Required Env Vars
- `FIRESTORE_ENABLED=1`
- `FIRESTORE_COLLECTION=sage_queue` (optional, default is `sage_queue`)

## Permissions
The Cloud Run service account needs Firestore access:
- `roles/datastore.user` (or more restrictive if desired)

## Stored Fields (example)
- `payload_meta` (message id, subject, from, to)
- `attachment` (name, content type, size bytes)
- `parsed` (InvoiceData as dict)
- `status`, `sage`, `error`
