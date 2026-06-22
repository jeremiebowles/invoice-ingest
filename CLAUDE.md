# invoice-ingest — Claude notes

## Supplier contact ID lookup

Sage contact IDs are stored locally in `sage_contacts.json` (gitignored). To find a
supplier's contact ID, grep that file:

```
grep -i "supplier name" sage_contacts.json
```

The file is a JSON array of `{ "id": "...", "displayed_as": "..." }` objects.
Each parser hardcodes its supplier's `contact_id` using the UUID from that file.
