from datetime import datetime
from os import getenv
from zoneinfo import ZoneInfo
from xml.sax.saxutils import escape

from twilio.base.exceptions import TwilioException
from twilio.rest import Client

from services.secure_store_service import (
    get_all_users_decrypted,
    get_user_medicines_decrypted,
    mark_reminder_sent,
    reminder_was_sent,
    sync_missed_doses_for_user,
)

TWILIO_ACCOUNT_SID = getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = getenv("TWILIO_AUTH_TOKEN")
TWILIO_FROM_NUMBER = getenv("TWILIO_FROM_NUMBER") or getenv("TWILIO_PHONE_NUMBER")
APP_TIMEZONE = getenv("APP_TIMEZONE", "Asia/Kolkata")
DEFAULT_COUNTRY_CODE = getenv("DEFAULT_COUNTRY_CODE", "+91")
TWILIO_ENABLE_CALL_REMINDERS = getenv("TWILIO_ENABLE_CALL_REMINDERS", "false").lower() in {
    "1",
    "true",
    "yes",
}


def _twilio_ready():
    return bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM_NUMBER)


def _normalize_phone(phone):
    if not phone:
        return ""
    value = str(phone).strip().replace(" ", "").replace("-", "")

    # Already E.164-like.
    if value.startswith("+"):
        return value

    # India local format: 10-digit mobile => +91XXXXXXXXXX
    if value.isdigit() and len(value) == 10:
        return f"{DEFAULT_COUNTRY_CODE}{value}"

    # 91XXXXXXXXXX => +91XXXXXXXXXX
    if value.isdigit() and len(value) == 12 and value.startswith("91"):
        return f"+{value}"

    # Keep fallback as-is (Twilio may reject invalid format).
    return value


def _is_due_today(medicine, now):
    """
    Returns True if:
      - today is within [startDate, endDate] (inclusive). If endDate missing, treat it as startDate.
      - AND current hour/minute match the scheduled time.
    Missing dates or bad formats return False.
    """
    start_date = str(medicine.get("startDate") or "").strip()
    end_date = str(medicine.get("endDate") or "").strip()

    if not start_date:
        return False

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        return False

    if end_date:
        try:
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            end = start
    else:
        end = start

    today = now.date()
    if today < start or today > end:
        return False

    try:
        hour = int(medicine.get("timeHour"))
        minute = int(medicine.get("timeMinute"))
    except (TypeError, ValueError):
        return False

    return now.hour == hour and now.minute == minute


def _build_message(medicine):
    medicine_name = str(medicine.get("medicineName") or "your medicine").strip()
    dosage = str(medicine.get("dosage") or "").strip()
    if dosage:
        return (
            f"Reminder: It's time to take {medicine_name} ({dosage}). "
            "Please take your medicine on time and stay healthy."
        )
    return (
        f"Reminder: It's time to take {medicine_name}. "
        "Please take your medicine on time and stay healthy."
    )


def _build_voice_message(medicine):
    medicine_name = str(medicine.get("medicineName") or "your medicine").strip()
    dosage = str(medicine.get("dosage") or "").strip()
    hour = medicine.get("timeHour")
    minute = medicine.get("timeMinute")
    time_text = ""
    if hour is not None and minute is not None:
        time_text = f"{hour:02d}:{minute:02d}"

    if dosage and time_text:
        return (
            f"Hey, this is a reminder call from MediMind. "
            f"Take {medicine_name}, dosage {dosage}, at {time_text} time. "
            "Stay healthy and take care."
        )
    if dosage:
        return (
            f"Hey, this is a reminder call from MediMind. "
            f"Take {medicine_name}, dosage {dosage}. "
            "Stay healthy and take care."
        )
    return (
        f"Hey, this is a reminder call from MediMind. "
        f"Take {medicine_name} medicine. "
        "Stay healthy and take care."
    )


def _to_twiml_say(message):
    escaped = escape(message)
    return f"<Response><Say voice=\"alice\">{escaped}</Say></Response>"


def run_due_reminders():
    if not _twilio_ready():
        return {
            "ok": False,
            "error": "Twilio env vars are missing. Set TWILIO_ACCOUNT_SID, "
            "TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER.",
        }

    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    now = datetime.now(ZoneInfo(APP_TIMEZONE))
    today_key = now.strftime("%Y%m%d")
    minute_key = now.strftime("%H%M")

    sent = 0
    calls = 0
    skipped = 0
    errors = []

    users = get_all_users_decrypted()
    for user in users:
        role = str(user.get("role") or "").strip()
        if role != "Patient":
            continue

        # Keep patient dose status in sync even when app is not open.
        sync_missed_doses_for_user(user.get("userId"))

        phone = _normalize_phone(user.get("phoneNumber"))
        if not phone:
            skipped += 1
            continue

        medicines = get_user_medicines_decrypted(user.get("userId"))
        for medicine in medicines:
            if not _is_due_today(medicine, now):
                continue

            medicine_id = medicine.get("id") or "unknown"
            log_id = f"{user.get('userId')}_{medicine_id}_{today_key}_{minute_key}"
            if reminder_was_sent(log_id):
                skipped += 1
                continue

            message_body = _build_message(medicine)
            voice_body = _build_voice_message(medicine)
            try:
                msg = client.messages.create(
                    body=message_body,
                    from_=TWILIO_FROM_NUMBER,
                    to=phone,
                )
                call_sid = None
                call_status = None
                if TWILIO_ENABLE_CALL_REMINDERS:
                    call = client.calls.create(
                        twiml=_to_twiml_say(voice_body),
                        from_=TWILIO_FROM_NUMBER,
                        to=phone,
                    )
                    call_sid = call.sid
                    call_status = call.status
                    calls += 1

                mark_reminder_sent(
                    log_id,
                    {
                        "userId": user.get("userId"),
                        "medicineId": medicine_id,
                        "phoneNumber": phone,
                        "twilioSid": msg.sid,
                        "status": msg.status,
                        "voiceSid": call_sid,
                        "voiceStatus": call_status,
                        "scheduledDate": now.strftime("%Y-%m-%d"),
                        "scheduledTime": now.strftime("%H:%M"),
                    },
                )
                print(
                    f"[REMINDER] sent user={user.get('userId')} "
                    f"medicine={medicine_id} phone={phone} sid={msg.sid}"
                )
                sent += 1
            except TwilioException as exc:
                print(f"[REMINDER] twilio_error user={user.get('userId')} err={exc}")
                errors.append(str(exc))
            except Exception as exc:
                print(f"[REMINDER] error user={user.get('userId')} err={exc}")
                errors.append(str(exc))

    return {
        "ok": True,
        "sent": sent,
        "calls": calls,
        "skipped": skipped,
        "errors": errors[:10],
        "checkedAt": now.isoformat(),
    }
