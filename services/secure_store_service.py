import json
import secrets
import string
from datetime import datetime, timedelta
from os import getenv
from pathlib import Path
from zoneinfo import ZoneInfo

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
APP_TIMEZONE = getenv("APP_TIMEZONE", "Asia/Kolkata")
DOSE_PENDING_WINDOW_MINUTES = 5


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


def _app_now():
    return datetime.now(ZoneInfo(APP_TIMEZONE))


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_medicine_active_on_date(medicine, target_date):
    start_date = str(medicine.get("startDate") or "").strip()
    end_date = str(medicine.get("endDate") or "").strip()
    if not start_date or not end_date:
        return False
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        return False
    return start <= target_date <= end


def _get_scheduled_datetime_for_date(medicine, target_date):
    hour = _safe_int(medicine.get("timeHour"))
    minute = _safe_int(medicine.get("timeMinute"))
    if hour is None or minute is None or hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return datetime(
        year=target_date.year,
        month=target_date.month,
        day=target_date.day,
        hour=hour,
        minute=minute,
        tzinfo=ZoneInfo(APP_TIMEZONE),
    )


def _dose_doc_id(user_id, medicine_id, target_date, hour, minute):
    date_key = target_date.strftime("%Y%m%d")
    time_key = f"{hour:02d}{minute:02d}"
    return f"{user_id}_{medicine_id}_{date_key}_{time_key}"


def _dose_status_collection():
    return _db.collection("medicine_dose_status")


def _get_dose_status_doc(user_id, medicine_id, target_date, hour, minute):
    doc_id = _dose_doc_id(user_id, medicine_id, target_date, hour, minute)
    return _dose_status_collection().document(doc_id).get()


def _set_dose_status(user_id, medicine_id, target_date, hour, minute, status, source):
    doc_id = _dose_doc_id(user_id, medicine_id, target_date, hour, minute)
    _dose_status_collection().document(doc_id).set(
        {
            "userId": user_id,
            "medicineId": medicine_id,
            "scheduledDate": target_date.strftime("%Y-%m-%d"),
            "scheduledTime": f"{hour:02d}:{minute:02d}",
            "status": status,
            "source": source,
            "markedAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def _get_status_map_for_user_on_date(user_id, scheduled_date):
    docs = (
        _dose_status_collection()
        .where("userId", "==", user_id)
        .where("scheduledDate", "==", scheduled_date)
        .stream()
    )
    mapping = {}
    for doc in docs:
        data = doc.to_dict()
        medicine_id = str(data.get("medicineId") or "").strip()
        if not medicine_id:
            continue
        mapping[medicine_id] = str(data.get("status") or "").strip()
    return mapping


def _delete_dose_status_docs_for_medicine(user_id, medicine_id):
    docs = (
        _dose_status_collection()
        .where("userId", "==", user_id)
        .where("medicineId", "==", medicine_id)
        .stream()
    )
    for doc in docs:
        doc.reference.delete()


def _mark_missed_doses_for_user(user_id, now=None):
    current = now or _app_now()
    target_date = current.date()
    medicines = get_user_medicines_decrypted(user_id)
    for medicine in medicines:
        medicine_id = str(medicine.get("id") or "").strip()
        if not medicine_id:
            continue
        if not _is_medicine_active_on_date(medicine, target_date):
            continue
        scheduled = _get_scheduled_datetime_for_date(medicine, target_date)
        if scheduled is None:
            continue

        cutoff = scheduled + timedelta(minutes=DOSE_PENDING_WINDOW_MINUTES)
        if current <= cutoff:
            continue

        hour = _safe_int(medicine.get("timeHour"))
        minute = _safe_int(medicine.get("timeMinute"))
        if hour is None or minute is None:
            continue

        status_doc = _get_dose_status_doc(user_id, medicine_id, target_date, hour, minute)
        if status_doc.exists:
            continue
        _set_dose_status(
            user_id,
            medicine_id,
            target_date,
            hour,
            minute,
            "Missed",
            "auto-timeout",
        )


def sync_missed_doses_for_user(user_id):
    _mark_missed_doses_for_user(user_id, _app_now())


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


def _build_temp_password(length=20):
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _get_or_create_patient_auth_user(email):
    try:
        return auth.get_user_by_email(email), False
    except auth.UserNotFoundError:
        # Create an auth user so caregivers can onboard patients who have not signed up yet.
        display_name = email.split("@", 1)[0] if "@" in email else "Patient"
        created_user = auth.create_user(
            email=email,
            password=_build_temp_password(),
            display_name=display_name,
        )
        return created_user, True


def save_medicine(user, payload):
    target_patient_id = str(payload.get("targetPatientId") or "").strip()
    owner_user_id = user["uid"]

    if target_patient_id and target_patient_id != user["uid"]:
        caller_profile = get_user_profile(user)
        if caller_profile.get("role") != "Caregiver":
            return {"error": "Only caregiver can save medicine for another patient."}
        if not _is_caregiver_linked_to_patient(user["uid"], target_patient_id):
            return {"error": "Selected patient is not linked to this caregiver."}

        link_id = f"{user['uid']}_{target_patient_id}"
        _db.collection("caregiver_patient_links").document(link_id).set(
            {
                "updatedAt": firestore.SERVER_TIMESTAMP,
                "lastMedicineSavedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
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
    caller_profile = get_user_profile(user)
    caller_role = str(caller_profile.get("role") or "").strip()
    now = _app_now()
    scheduled_date = now.strftime("%Y-%m-%d")

    if caller_role == "Caregiver":
        links = (
            _db.collection("caregiver_patient_links")
            .where("caregiverId", "==", user["uid"])
            .stream()
        )

        patient_relation_map = {}
        patient_profile_map = {}
        for link in links:
            link_data = link.to_dict()
            patient_id = str(link_data.get("patientId") or "").strip()
            if not patient_id:
                continue
            patient_relation_map[patient_id] = str(link_data.get("relation") or "").strip()
            patient_profile_map[patient_id] = _get_user_profile_by_uid(patient_id)

        patient_ids = list(patient_relation_map.keys())
        if not patient_ids:
            return {"items": []}

        rows = []
        status_cache = {}
        for idx in range(0, len(patient_ids), 10):
            chunk = patient_ids[idx:idx + 10]
            docs = _db.collection("medicines_secure").where("userId", "in", chunk).stream()
            for doc in docs:
                raw = doc.to_dict()
                owner_user_id = str(raw.get("userId") or "").strip()
                data = _decrypt_payload(raw.get("dataEnc"))
                data["id"] = doc.id
                data["patientRelation"] = patient_relation_map.get(owner_user_id, "")
                owner_profile = patient_profile_map.get(owner_user_id) or {}
                data["patientUserId"] = owner_user_id
                data["patientEmail"] = owner_profile.get("email")
                data["patientName"] = owner_profile.get("displayName")
                if owner_user_id not in status_cache:
                    _mark_missed_doses_for_user(owner_user_id, now)
                    status_cache[owner_user_id] = _get_status_map_for_user_on_date(
                        owner_user_id,
                        scheduled_date,
                    )
                data["todayStatus"] = status_cache[owner_user_id].get(doc.id, "")
                created_at = raw.get("createdAt")
                data["_createdAtTs"] = (
                    created_at.timestamp() if hasattr(created_at, "timestamp") else 0.0
                )
                rows.append(data)

        rows.sort(key=lambda row: row.get("_createdAtTs", 0.0), reverse=True)
        for row in rows:
            row.pop("_createdAtTs", None)
        return {"items": rows}

    docs = (
        _db.collection("medicines_secure")
        .where("userId", "==", user["uid"])
        .stream()
    )
    _mark_missed_doses_for_user(user["uid"], now)
    today_status_map = _get_status_map_for_user_on_date(user["uid"], scheduled_date)
    rows = []
    for doc in docs:
        raw = doc.to_dict()
        data = _decrypt_payload(raw.get("dataEnc"))
        data["id"] = doc.id
        data["todayStatus"] = today_status_map.get(doc.id, "")
        created_at = raw.get("createdAt")
        data["_createdAtTs"] = (
            created_at.timestamp() if hasattr(created_at, "timestamp") else 0.0
        )
        rows.append(data)

    rows.sort(key=lambda row: row.get("_createdAtTs", 0.0), reverse=True)
    for row in rows:
        row.pop("_createdAtTs", None)
    return {"items": rows}


def delete_medicine(user, medicine_id):
    medicine_id_value = str(medicine_id or "").strip()
    if not medicine_id_value:
        return {"error": "Medicine id is required."}

    doc_ref = _db.collection("medicines_secure").document(medicine_id_value)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": "Medicine not found."}

    raw = doc.to_dict()
    owner_user_id = str(raw.get("userId") or "").strip()
    role = str(get_user_profile(user).get("role") or "").strip()
    can_delete = owner_user_id == user["uid"]
    if not can_delete and role == "Caregiver":
        can_delete = _is_caregiver_linked_to_patient(user["uid"], owner_user_id)
    if not can_delete:
        return {"error": "Not allowed to delete this medicine."}

    doc_ref.delete()
    _delete_dose_status_docs_for_medicine(owner_user_id, medicine_id_value)
    return {"ok": True}


def clear_medicine_history(user):
    role = str(get_user_profile(user).get("role") or "").strip()
    if role not in {"Patient", "Caregiver"}:
        return {"error": "Clear history is available for Patient or Caregiver only."}

    target_user_ids = []
    if role == "Patient":
        target_user_ids = [user["uid"]]
    else:
        links = (
            _db.collection("caregiver_patient_links")
            .where("caregiverId", "==", user["uid"])
            .stream()
        )
        for link in links:
            link_data = link.to_dict()
            patient_id = str(link_data.get("patientId") or "").strip()
            if patient_id:
                target_user_ids.append(patient_id)

    target_user_ids = list(dict.fromkeys(target_user_ids))
    if not target_user_ids:
        return {"ok": True, "deletedMedicines": 0}

    deleted_medicine_ids = []
    deleted_count = 0
    for idx in range(0, len(target_user_ids), 10):
        chunk = target_user_ids[idx:idx + 10]
        docs = _db.collection("medicines_secure").where("userId", "in", chunk).stream()
        for doc in docs:
            deleted_medicine_ids.append((str(doc.to_dict().get("userId") or ""), doc.id))
            doc.reference.delete()
            deleted_count += 1

    for owner_user_id, medicine_id in deleted_medicine_ids:
        if owner_user_id and medicine_id:
            _delete_dose_status_docs_for_medicine(owner_user_id, medicine_id)

    return {"ok": True, "deletedMedicines": deleted_count}


def list_today_pending_medicines(user):
    profile = get_user_profile(user)
    role = str(profile.get("role") or "").strip()
    if role != "Patient":
        return {"items": []}

    now = _app_now()
    target_date = now.date()
    _mark_missed_doses_for_user(user["uid"], now)

    items = []
    medicines = get_user_medicines_decrypted(user["uid"])
    for medicine in medicines:
        medicine_id = str(medicine.get("id") or "").strip()
        if not medicine_id:
            continue
        if not _is_medicine_active_on_date(medicine, target_date):
            continue

        hour = _safe_int(medicine.get("timeHour"))
        minute = _safe_int(medicine.get("timeMinute"))
        if hour is None or minute is None:
            continue

        scheduled = _get_scheduled_datetime_for_date(medicine, target_date)
        if scheduled is None:
            continue
        window_end = scheduled + timedelta(minutes=DOSE_PENDING_WINDOW_MINUTES)
        if now < scheduled or now > window_end:
            continue

        status_doc = _get_dose_status_doc(user["uid"], medicine_id, target_date, hour, minute)
        if status_doc.exists:
            status_value = str(status_doc.to_dict().get("status") or "").strip()
            if status_value in {"Taken", "Missed"}:
                continue

        items.append(
            {
                "medicineId": medicine_id,
                "medicineName": medicine.get("medicineName"),
                "dosage": medicine.get("dosage"),
                "startDate": medicine.get("startDate"),
                "timeHour": hour,
                "timeMinute": minute,
                "windowEndAt": window_end.isoformat(),
            }
        )

    items.sort(
        key=lambda item: (
            _safe_int(item.get("timeHour")) or 0,
            _safe_int(item.get("timeMinute")) or 0,
        )
    )
    return {"items": items}


def mark_medicine_taken(user, medicine_id):
    profile = get_user_profile(user)
    role = str(profile.get("role") or "").strip()
    if role != "Patient":
        return {"error": "Only Patient can mark medicine as taken."}

    medicine_id_value = str(medicine_id or "").strip()
    if not medicine_id_value:
        return {"error": "Medicine id is required."}

    doc = _db.collection("medicines_secure").document(medicine_id_value).get()
    if not doc.exists:
        return {"error": "Medicine not found."}
    raw = doc.to_dict()
    owner_user_id = str(raw.get("userId") or "").strip()
    if owner_user_id != user["uid"]:
        return {"error": "You can only mark your own medicine as taken."}

    medicine = _decrypt_payload(raw.get("dataEnc"))
    now = _app_now()
    target_date = now.date()
    if not _is_medicine_active_on_date(medicine, target_date):
        return {"error": "Medicine is not scheduled for today."}

    hour = _safe_int(medicine.get("timeHour"))
    minute = _safe_int(medicine.get("timeMinute"))
    if hour is None or minute is None:
        return {"error": "Invalid medicine time."}

    scheduled = _get_scheduled_datetime_for_date(medicine, target_date)
    if scheduled is None:
        return {"error": "Invalid medicine schedule."}
    window_end = scheduled + timedelta(minutes=DOSE_PENDING_WINDOW_MINUTES)
    if now < scheduled:
        return {"error": "You can mark as taken only from reminder time."}
    if now > window_end:
        _mark_missed_doses_for_user(user["uid"], now)
        return {"error": "Marked as missed (time window exceeded)."}

    status_doc = _get_dose_status_doc(user["uid"], medicine_id_value, target_date, hour, minute)
    if status_doc.exists:
        status_value = str(status_doc.to_dict().get("status") or "").strip()
        if status_value == "Taken":
            return {"ok": True, "status": "Taken"}
        if status_value == "Missed":
            return {"error": "Medicine already marked as missed."}

    _set_dose_status(
        user["uid"],
        medicine_id_value,
        target_date,
        hour,
        minute,
        "Taken",
        "patient-button",
    )
    return {"ok": True, "status": "Taken"}


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


def add_patient_for_caregiver(user, patient_email, patient_phone, patient_relation):
    caller_profile = get_user_profile(user)
    if caller_profile.get("role") != "Caregiver":
        return {"error": "Only caregiver can add patients."}

    email = str(patient_email or "").strip().lower()
    if not email:
        return {"error": "Patient email is required."}
    relation = str(patient_relation or "").strip()
    if not relation:
        return {"error": "Patient relation is required."}

    try:
        patient_auth, was_auto_created = _get_or_create_patient_auth_user(email)
    except Exception as exc:
        return {"error": f"Unable to add patient in Firebase: {exc}"}

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
            "relation": relation,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    return {
        "ok": True,
        "wasAutoCreatedInAuth": was_auto_created,
        "patient": {
            "userId": patient_id,
            "email": payload.get("email"),
            "displayName": payload.get("displayName"),
            "phoneNumber": payload.get("phoneNumber"),
            "role": "Patient",
            "relation": relation,
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

    patient_rows = []
    for link in links:
        link_data = link.to_dict()
        patient_id = link_data.get("patientId")
        if not patient_id:
            continue
        updated_at = link_data.get("updatedAt")
        sort_key = updated_at.timestamp() if hasattr(updated_at, "timestamp") else 0.0
        relation = str(link_data.get("relation") or "").strip()
        patient_rows.append((sort_key, patient_id, relation))

    items = []
    for _, patient_id, relation in sorted(
        patient_rows,
        key=lambda row: row[0],
        reverse=True,
    ):
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
                "relation": relation,
            }
        )

    return {"items": items}
