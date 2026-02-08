from __future__ import annotations

import os
from typing import Any, Dict, Optional

from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists


def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value.strip() if value else None


def _get_database() -> Optional[str]:
    value = _get_env("FIRESTORE_DATABASE")
    return value or None


def _get_collection():
    collection = _get_env("FIRESTORE_COLLECTION") or "sage_queue"
    database = _get_database()
    client = firestore.Client(database=database)
    return client.collection(collection)


def _get_message_collection():
    collection = _get_env("FIRESTORE_MESSAGE_COLLECTION") or "processed_messages"
    database = _get_database()
    client = firestore.Client(database=database)
    return client.collection(collection)


def _normalize_message_id(message_id: str) -> str:
    return message_id.strip().replace("/", "_")


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


def get_latest_parsed_record(limit: int = 10) -> Optional[tuple[str, Dict[str, Any]]]:
    col = _get_collection()
    query = col.order_by("created_at", direction=firestore.Query.DESCENDING).limit(limit)
    for doc in query.stream():
        data = doc.to_dict() or {}
        if data.get("parsed"):
            return doc.id, data
    return None


def get_latest_record() -> Optional[tuple[str, Dict[str, Any]]]:
    col = _get_collection()
    query = col.order_by("created_at", direction=firestore.Query.DESCENDING).limit(1)
    for doc in query.stream():
        return doc.id, doc.to_dict() or {}
    return None


def test_roundtrip() -> Dict[str, Any]:
    col = _get_collection()
    doc_ref = col.document()
    payload = {
        "status": "firestore_test",
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }
    doc_ref.set(payload)
    snapshot = doc_ref.get()
    return {
        "id": doc_ref.id,
        "exists": snapshot.exists,
        "data": snapshot.to_dict() or {},
    }


def list_records(limit: int = 5) -> list[Dict[str, Any]]:
    col = _get_collection()
    results: list[Dict[str, Any]] = []
    for doc in col.limit(limit).stream():
        results.append({"id": doc.id, "data": doc.to_dict() or {}})
    return results


def find_records_by_reference(reference: str, limit: int = 20) -> list[Dict[str, Any]]:
    if not reference:
        return []
    col = _get_collection()
    results: list[Dict[str, Any]] = []
    query = col.where("parsed.supplier_reference", "==", reference).limit(limit)
    for doc in query.stream():
        results.append({"id": doc.id, "data": doc.to_dict() or {}})
    return results


def get_client_info() -> Dict[str, Any]:
    collection = _get_env("FIRESTORE_COLLECTION") or "sage_queue"
    database = _get_database() or "(default)"
    client = firestore.Client(database=_get_database())
    return {"project": client.project, "database": database, "collection": collection}


def has_recent_posted_match(
    supplier_reference: Optional[str],
    invoice_date: Optional[str],
    is_credit: Optional[bool],
    limit: int = 200,
) -> bool:
    if not supplier_reference or not invoice_date:
        return False
    col = _get_collection()
    query = col.order_by("created_at", direction=firestore.Query.DESCENDING).limit(limit)
    for doc in query.stream():
        data = doc.to_dict() or {}
        if data.get("status") != "posted":
            continue
        parsed = data.get("parsed") or {}
        if parsed.get("supplier_reference") != supplier_reference:
            continue
        if parsed.get("invoice_date") != invoice_date:
            continue
        if is_credit is not None and parsed.get("is_credit") != is_credit:
            continue
        return True
    return False


def increment_rate_limit(key: str, limit: int) -> int:
    collection = _get_env("FIRESTORE_RATE_LIMIT_COLLECTION") or "rate_limits"
    database = _get_database()
    client = firestore.Client(database=database)
    doc_ref = client.collection(collection).document(key)
    doc_ref.set(
        {"count": firestore.Increment(1), "updated_at": firestore.SERVER_TIMESTAMP},
        merge=True,
    )
    snapshot = doc_ref.get()
    data = snapshot.to_dict() or {}
    count = int(data.get("count") or 0)
    if limit > 0 and count > limit:
        return count
    return count


def reserve_message_id(message_id: str, data: Optional[Dict[str, Any]] = None) -> bool:
    if not message_id:
        return False
    col = _get_message_collection()
    doc_id = _normalize_message_id(message_id)
    payload = dict(data or {})
    payload.setdefault("created_at", firestore.SERVER_TIMESTAMP)
    payload.setdefault("updated_at", firestore.SERVER_TIMESTAMP)
    try:
        col.document(doc_id).create(payload)
        return True
    except AlreadyExists:
        return False


def update_message_status(message_id: str, fields: Dict[str, Any]) -> None:
    if not message_id:
        return
    col = _get_message_collection()
    doc_id = _normalize_message_id(message_id)
    payload = dict(fields)
    payload["updated_at"] = firestore.SERVER_TIMESTAMP
    col.document(doc_id).set(payload, merge=True)
