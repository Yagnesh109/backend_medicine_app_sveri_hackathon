import base64
import json
from os import getenv

import requests

GEMINI_API_KEY = getenv("GEMINI_API_KEY")
GEMINI_MODEL = getenv("GEMINI_MODEL", "gemini-1.5-flash")


def _extract_json_from_text(text):
    if not text:
        return None

    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json", "", 1).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            return json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError:
            return None


def extract_medicine_details_from_image(image_bytes, mime_type):
    if not GEMINI_API_KEY:
        return {"error": "GEMINI_API_KEY is not configured on backend."}

    prompt = (
        "Extract medicine schedule information from this prescription/medicine image. "
        "Return only valid JSON object with exactly these keys: "
        "medicineName, dosage, startDate, endDate, time, mealType, mealRelation. "
        "Date format must be YYYY-MM-DD when available. "
        "Time format should be HH:MM (24-hour) when available. "
        "mealType must be one of Breakfast, Lunch, Dinner. "
        "mealRelation must be one of Before Meal, After Meal. "
        "Use empty string for unavailable fields."
    )

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt},
                    {
                        "inline_data": {
                            "mime_type": mime_type or "image/jpeg",
                            "data": image_b64,
                        }
                    },
                ]
            }
        ]
    }

    try:
        response = requests.post(url, json=payload, timeout=40)
    except requests.RequestException as exc:
        return {"error": f"Gemini request failed: {exc}"}

    if response.status_code != 200:
        return {"error": f"Gemini API error {response.status_code}: {response.text}"}

    data = response.json()
    candidates = data.get("candidates", [])
    if not candidates:
        return {"error": "No response candidates from Gemini."}

    parts = candidates[0].get("content", {}).get("parts", [])
    text_part = ""
    for part in parts:
        if "text" in part:
            text_part += part["text"]

    parsed = _extract_json_from_text(text_part)
    if not parsed or not isinstance(parsed, dict):
        return {"error": "Could not parse structured JSON from Gemini response."}

    return {
        "medicineName": str(parsed.get("medicineName", "")).strip(),
        "dosage": str(parsed.get("dosage", "")).strip(),
        "startDate": str(parsed.get("startDate", "")).strip(),
        "endDate": str(parsed.get("endDate", "")).strip(),
        "time": str(parsed.get("time", "")).strip(),
        "mealType": str(parsed.get("mealType", "")).strip(),
        "mealRelation": str(parsed.get("mealRelation", "")).strip(),
    }
