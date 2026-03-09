import json
from os import getenv
from pathlib import Path

import firebase_admin
from cryptography.fernet import Fernet, InvalidToken
from fastapi import Header, HTTPException
from firebase_admin import auth, credentials, firestore


def _init_firebase():
    if firebase_admin._apps:
        return

    service_account_json = getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
    if service_account_json:
        try:
            service_account_info = json.loads(service_account_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_JSON is not valid JSON.") from exc
        cred = credentials.Certificate(service_account_info)
        firebase_admin.initialize_app(cred)
        return

    service_account_path = getenv(
        "FIREBASE_SERVICE_ACCOUNT_PATH",
        "serviceAccountKey.json",
    )
    path = Path(service_account_path)
    if not path.exists():
        raise RuntimeError(
            "Firebase service account not found. "
            "Set FIREBASE_SERVICE_ACCOUNT_JSON or provide FIREBASE_SERVICE_ACCOUNT_PATH."
        )

    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred)


_init_firebase()
_db = firestore.client()

_enc_key = getenv("DATA_ENCRYPTION_KEY")
if not _enc_key:
    raise RuntimeError("DATA_ENCRYPTION_KEY is required.")
_fernet = Fernet(_enc_key.encode("utf-8"))


def _encrypt_text(value):
    raw = "" if value is None else str(value)
    return _fernet.encrypt(raw.encode("utf-8")).decode("utf-8")


def _decrypt_text(value):
    if value is None:
        return ""
    try:
        return _fernet.decrypt(value.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        return ""


def _encrypt_payload(payload):
    return _encrypt_text(json.dumps(payload))


def _decrypt_payload(value):
    try:
        decrypted = _decrypt_text(value)
        if not decrypted:
            return {}
        return json.loads(decrypted)
    except json.JSONDecodeError:
        return {}


def get_current_user(
    authorization: str = Header(default=None),
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid bearer token")

    try:
        decoded = auth.verify_id_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid Firebase token")

    uid = decoded.get("uid")
    if not uid:
        raise HTTPException(status_code=401, detail="Token missing uid")

    return {
        "uid": uid,
        "email": decoded.get("email"),
        "name": decoded.get("name"),
        "picture": decoded.get("picture"),
    }


def set_user_role(user, role):
    role_value = str(role).strip()
    if role_value not in {"Patient", "Caregiver", "Doctor"}:
        return {"error": "Invalid role value."}

    doc_ref = _db.collection("users_secure").document(user["uid"])
    existing = doc_ref.get()
    existing_data = {}
    if existing.exists:
        existing_data = _decrypt_payload(existing.to_dict().get("dataEnc"))

    payload = {
        "role": role_value,
        "email": user.get("email"),
        "displayName": user.get("name"),
        "photoURL": user.get("picture"),
        "phoneNumber": existing_data.get("phoneNumber", ""),
    }
    encrypted_payload = _encrypt_payload(payload)

    doc_ref.set(
        {
            "userId": user["uid"],
            "dataEnc": encrypted_payload,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    return {"ok": True, "role": role_value}


def set_user_phone(user, phone_number):
    phone_value = str(phone_number or "").strip()
    if not phone_value:
        return {"error": "Phone number is required."}

    doc_ref = _db.collection("users_secure").document(user["uid"])
    existing = doc_ref.get()
    existing_data = {}
    if existing.exists:
        existing_data = _decrypt_payload(existing.to_dict().get("dataEnc"))

    payload = {
        "role": existing_data.get("role"),
        "email": user.get("email") or existing_data.get("email"),
        "displayName": user.get("name") or existing_data.get("displayName"),
        "photoURL": user.get("picture") or existing_data.get("photoURL"),
        "phoneNumber": phone_value,
    }
    encrypted_payload = _encrypt_payload(payload)

    doc_ref.set(
        {
            "userId": user["uid"],
            "dataEnc": encrypted_payload,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    return {"ok": True, "phoneNumber": phone_value}


def get_user_profile(user):
    doc_ref = _db.collection("users_secure").document(user["uid"])
    snapshot = doc_ref.get()
    if not snapshot.exists:
        return {"role": None}

    enc = snapshot.to_dict().get("dataEnc")
    data = _decrypt_payload(enc)
    return {
        "role": data.get("role"),
        "email": data.get("email"),
        "displayName": data.get("displayName"),
        "photoURL": data.get("photoURL"),
        "phoneNumber": data.get("phoneNumber"),
    }


def save_medicine(user, payload):
    encrypted_payload = _encrypt_payload(payload)
    doc_ref = _db.collection("medicines_secure").document()
    doc_ref.set(
        {
            "userId": user["uid"],
            "dataEnc": encrypted_payload,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )
    return {"ok": True, "id": doc_ref.id}


def list_medicines(user):
    docs = (
        _db.collection("medicines_secure")
        .where("userId", "==", user["uid"])
        .stream()
    )
    rows = []
    for doc in docs:
        raw = doc.to_dict()
        data = _decrypt_payload(raw.get("dataEnc"))
        data["id"] = doc.id
        rows.append(data)
    return {"items": rows}


def get_all_users_decrypted():
    docs = _db.collection("users_secure").stream()
    rows = []
    for doc in docs:
        raw = doc.to_dict()
        data = _decrypt_payload(raw.get("dataEnc"))
        rows.append(
            {
                "userId": raw.get("userId") or doc.id,
                "role": data.get("role"),
                "phoneNumber": data.get("phoneNumber"),
                "email": data.get("email"),
                "displayName": data.get("displayName"),
            }
        )
    return rows


def get_user_medicines_decrypted(user_id):
    docs = (
        _db.collection("medicines_secure")
        .where("userId", "==", user_id)
        .stream()
    )
    rows = []
    for doc in docs:
        raw = doc.to_dict()
        data = _decrypt_payload(raw.get("dataEnc"))
        data["id"] = doc.id
        rows.append(data)
    return rows


def reminder_was_sent(log_id):
    return _db.collection("reminder_logs").document(log_id).get().exists


def mark_reminder_sent(log_id, payload):
    _db.collection("reminder_logs").document(log_id).set(
        {
            **payload,
            "sentAt": firestore.SERVER_TIMESTAMP,
        }
    )


def get_recent_reminder_logs(limit=50):
    docs = (
        _db.collection("reminder_logs")
        .order_by("sentAt", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .stream()
    )
    rows = []
    for doc in docs:
        raw = doc.to_dict()
        raw["id"] = doc.id
        rows.append(raw)
    return rows
