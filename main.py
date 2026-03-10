from dotenv import load_dotenv
from fastapi import Depends, FastAPI
from fastapi import File, UploadFile
from fastapi import Header, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from pydantic import BaseModel
from os import getenv

load_dotenv()

from services.medicine_service import get_medicine, get_medicine_by_barcode
from services.ocr_service import extract_medicine_details_from_image
from services.secure_store_service import (
    add_patient_for_caregiver,
    clear_medicine_history,
    delete_patient_for_caregiver,
    delete_medicine,
    get_current_user,
    get_recent_reminder_logs,
    get_user_profile,
    list_caregiver_patients,
    list_medicines,
    list_today_medicine_summary,
    list_today_pending_medicines,
    mark_medicine_taken,
    save_medicine,
    set_user_phone,
    set_user_role,
)
from services.reminder_service import run_due_reminders

app = FastAPI()
scheduler = BackgroundScheduler()

REMINDER_TRIGGER_KEY = getenv("REMINDER_TRIGGER_KEY", "")


class RolePayload(BaseModel):
    role: str


class MedicinePayload(BaseModel):
    medicineName: str
    dosage: str
    startDate: str
    endDate: str
    timeHour: int
    timeMinute: int
    mealType: str
    mealRelation: str
    source: str | None = None
    targetPatientId: str | None = None


class PhonePayload(BaseModel):
    phoneNumber: str


class CaregiverAddPatientPayload(BaseModel):
    patientEmail: str
    patientPhoneNumber: str
    patientRelation: str

@app.get("/medicine")
def medicine(name: str):
    return get_medicine(name)

@app.get("/medicine/barcode")
def medicine_by_barcode(code: str):
    return get_medicine_by_barcode(code)

@app.post("/medicine/extract-ocr")
async def extract_medicine_ocr(file: UploadFile = File(...)):
    image_bytes = await file.read()
    if not image_bytes:
        return {"error": "Empty image file."}
    return extract_medicine_details_from_image(image_bytes, file.content_type)


@app.get("/secure/user/profile")
def secure_user_profile(user=Depends(get_current_user)):
    return get_user_profile(user)


@app.post("/secure/user/role")
def secure_user_role(payload: RolePayload, user=Depends(get_current_user)):
    return set_user_role(user, payload.role)


@app.post("/secure/user/phone")
def secure_user_phone(payload: PhonePayload, user=Depends(get_current_user)):
    return set_user_phone(user, payload.phoneNumber)


@app.post("/secure/caregiver/patients")
def secure_add_patient_for_caregiver(
    payload: CaregiverAddPatientPayload,
    user=Depends(get_current_user),
):
    return add_patient_for_caregiver(
        user,
        payload.patientEmail,
        payload.patientPhoneNumber,
        payload.patientRelation,
    )


@app.get("/secure/caregiver/patients")
def secure_list_caregiver_patients(user=Depends(get_current_user)):
    return list_caregiver_patients(user)


@app.delete("/secure/caregiver/patients/{patient_id}")
def secure_delete_patient_for_caregiver(patient_id: str, user=Depends(get_current_user)):
    return delete_patient_for_caregiver(user, patient_id)


@app.post("/secure/medicine")
def secure_save_medicine(payload: MedicinePayload, user=Depends(get_current_user)):
    return save_medicine(user, payload.model_dump())


@app.get("/secure/medicines")
def secure_list_medicines(user=Depends(get_current_user)):
    return list_medicines(user)


@app.get("/secure/medicines/pending-today")
def secure_list_today_pending_medicines(user=Depends(get_current_user)):
    return list_today_pending_medicines(user)


@app.get("/secure/medicines/today-summary")
def secure_list_today_medicine_summary(user=Depends(get_current_user)):
    return list_today_medicine_summary(user)


@app.post("/secure/medicines/{medicine_id}/taken")
def secure_mark_medicine_taken(medicine_id: str, user=Depends(get_current_user)):
    return mark_medicine_taken(user, medicine_id)


@app.delete("/secure/medicines/history")
def secure_clear_medicine_history(user=Depends(get_current_user)):
    return clear_medicine_history(user)


@app.delete("/secure/medicines/{medicine_id}")
def secure_delete_medicine(medicine_id: str, user=Depends(get_current_user)):
    return delete_medicine(user, medicine_id)


@app.post("/secure/reminders/run")
def secure_run_reminders(x_trigger_key: str = Header(default="")):
    if REMINDER_TRIGGER_KEY and x_trigger_key != REMINDER_TRIGGER_KEY:
        raise HTTPException(status_code=401, detail="Invalid reminder trigger key")
    return run_due_reminders()


@app.get("/secure/reminders/logs")
def secure_reminder_logs(limit: int = 50, x_trigger_key: str = Header(default="")):
    if REMINDER_TRIGGER_KEY and x_trigger_key != REMINDER_TRIGGER_KEY:
        raise HTTPException(status_code=401, detail="Invalid reminder trigger key")
    safe_limit = max(1, min(limit, 200))
    return {"items": get_recent_reminder_logs(safe_limit)}


@app.on_event("startup")
def start_scheduler():
    if not scheduler.running:
        scheduler.add_job(run_due_reminders, "interval", minutes=1, id="sms-reminders")
        scheduler.start()


@app.on_event("shutdown")
def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
