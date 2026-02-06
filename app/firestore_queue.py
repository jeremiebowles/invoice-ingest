from __future__ import annotations

import os
from typing import Any, Dict, Optional

from google.cloud import firestore


def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value.strip() if value else None


def _get_collection():
    collection = _get_env("FIRESTORE_COLLECTION") or "sage_queue"
    client = firestore.Client()
    return client.collection(collection)


def enqueue_record(record: Dict[str, Any]) -> str:
    col = _get_collection()
    doc_ref = col.document()
    record = dict(record)
    record["created_at"] = firestore.SERVER_TIMESTAMP
    record["updated_at"] = firestore.SERVER_TIMESTAMP
    doc_ref.set(record)
    return doc_ref.id


def update_record(doc_id: str, fields: Dict[str, Any]) -> None:
    col = _get_collection()
    fields = dict(fields)
    fields["updated_at"] = firestore.SERVER_TIMESTAMP
    col.document(doc_id).set(fields, merge=True)
