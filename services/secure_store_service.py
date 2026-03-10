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


def _get_user_secure_doc(uid):
    return _db.collection("users_secure").document(uid)


def _get_user_profile_by_uid(uid):
    doc = _get_user_secure_doc(uid).get()
    if not doc.exists:
        return {}
    return _decrypt_payload(doc.to_dict().get("dataEnc"))


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

    doc_ref = _get_user_secure_doc(user["uid"])
    existing_data = _get_user_profile_by_uid(user["uid"])

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

    doc_ref = _get_user_secure_doc(user["uid"])
    existing_data = _get_user_profile_by_uid(user["uid"])

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
    data = _get_user_profile_by_uid(user["uid"])
    if not data:
        return {"role": None}
    return {
        "role": data.get("role"),
        "email": data.get("email"),
        "displayName": data.get("displayName"),
        "photoURL": data.get("photoURL"),
        "phoneNumber": data.get("phoneNumber"),
    }


def _is_caregiver_linked_to_patient(caregiver_id, patient_id):
    link_id = f"{caregiver_id}_{patient_id}"
    return _db.collection("caregiver_patient_links").document(link_id).get().exists


def save_medicine(user, payload):
    target_patient_id = str(payload.get("targetPatientId") or "").strip()
    owner_user_id = user["uid"]

    if target_patient_id and target_patient_id != user["uid"]:
        caller_profile = get_user_profile(user)
        if caller_profile.get("role") != "Caregiver":
            return {"error": "Only caregiver can save medicine for another patient."}
        if not _is_caregiver_linked_to_patient(user["uid"], target_patient_id):
            return {"error": "Selected patient is not linked to this caregiver."}
        owner_user_id = target_patient_id

    encrypted_payload = _encrypt_payload(payload)
    doc_ref = _db.collection("medicines_secure").document()
    doc_ref.set(
        {
            "userId": owner_user_id,
            "createdByUserId": user["uid"],
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


def add_patient_for_caregiver(user, patient_email, patient_phone):
    caller_profile = get_user_profile(user)
    if caller_profile.get("role") != "Caregiver":
        return {"error": "Only caregiver can add patients."}

    email = str(patient_email or "").strip().lower()
    if not email:
        return {"error": "Patient email is required."}

    try:
        patient_auth = auth.get_user_by_email(email)
    except Exception:
        return {"error": "No Firebase user found with this patient email."}

    patient_id = patient_auth.uid
    if patient_id == user["uid"]:
        return {"error": "Caregiver cannot add self as patient."}

    patient_profile = _get_user_profile_by_uid(patient_id)
    role = str(patient_profile.get("role") or "").strip()
    if role and role != "Patient":
        return {"error": "Selected user role is not Patient."}

    phone = str(patient_phone or "").strip()
    payload = {
        "role": "Patient",
        "email": patient_auth.email or email,
        "displayName": patient_auth.display_name,
        "photoURL": patient_auth.photo_url,
        "phoneNumber": phone or patient_profile.get("phoneNumber", ""),
    }

    _get_user_secure_doc(patient_id).set(
        {
            "userId": patient_id,
            "dataEnc": _encrypt_payload(payload),
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    link_id = f"{user['uid']}_{patient_id}"
    _db.collection("caregiver_patient_links").document(link_id).set(
        {
            "caregiverId": user["uid"],
            "patientId": patient_id,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    return {
        "ok": True,
        "patient": {
            "userId": patient_id,
            "email": payload.get("email"),
            "displayName": payload.get("displayName"),
            "phoneNumber": payload.get("phoneNumber"),
            "role": "Patient",
        },
    }


def list_caregiver_patients(user):
    caller_profile = get_user_profile(user)
    if caller_profile.get("role") != "Caregiver":
        return {"items": []}

    links = (
        _db.collection("caregiver_patient_links")
        .where("caregiverId", "==", user["uid"])
        .stream()
    )

    items = []
    for link in links:
        link_data = link.to_dict()
        patient_id = link_data.get("patientId")
        if not patient_id:
            continue
        patient_profile = _get_user_profile_by_uid(patient_id)
        if not patient_profile:
            continue
        items.append(
            {
                "userId": patient_id,
                "email": patient_profile.get("email"),
                "displayName": patient_profile.get("displayName"),
                "phoneNumber": patient_profile.get("phoneNumber"),
                "role": patient_profile.get("role"),
            }
        )

    return {"items": items}
