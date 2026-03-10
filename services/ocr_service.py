import base64
import json
import mimetypes
from os import getenv

import requests

GEMINI_API_KEY = getenv("GEMINI_API_KEY")
GEMINI_MODEL = getenv("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_API_VERSION = getenv("GEMINI_API_VERSION", "v1").strip() or "v1"
OPENROUTER_API_KEY = getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash")
OPENROUTER_REFERRER = getenv("OPENROUTER_REFERRER", "")
OPENROUTER_TITLE = getenv("OPENROUTER_TITLE", "MediMind OCR")


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
        # Try to find first JSON object substring.
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            return json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError:
            return None


def _gemini_models_url(api_version):
    return f"https://generativelanguage.googleapis.com/{api_version}/models"


def _gemini_generate_url(api_version, model, api_key):
    return (
        f"https://generativelanguage.googleapis.com/{api_version}/models/"
        f"{model}:generateContent?key={api_key}"
    )


def _try_list_models(api_version, api_key):
    try:
        res = requests.get(_gemini_models_url(api_version), params={"key": api_key}, timeout=20)
    except requests.RequestException:
        return []

    if res.status_code != 200:
        return []

    try:
        data = res.json()
    except Exception:
        return []

    models = data.get("models") or []
    out = []
    for m in models:
        name = str(m.get("name") or "").strip()
        if not name:
            continue
        methods = m.get("supportedGenerationMethods") or []
        if isinstance(methods, list) and "generateContent" not in methods:
            continue
        # API returns names like "models/gemini-1.5-flash"; strip prefix for URL building.
        if name.startswith("models/"):
            name = name.split("/", 1)[1]
        out.append(name)
    return out


def _pick_fallback_model(available, preferred):
    preferred_value = str(preferred or "").strip()
    if preferred_value and preferred_value in available:
        return preferred_value

    # Common model aliases across versions; we'll pick the first one that exists.
    preference_order = [
        preferred_value,
        "gemini-2.0-flash",
        "gemini-2.0-flash-lite",
        "gemini-2.5-flash",
        "gemini-1.5-flash-latest",
        "gemini-1.5-pro-latest",
        "gemini-1.5-flash",
        "gemini-1.5-pro",
    ]

    for name in preference_order:
        if name and name in available:
            return name

    # Last resort: any model that contains "flash" then anything.
    for name in available:
        if "flash" in name.lower():
            return name
    return available[0] if available else preferred_value


def _post_gemini_generate(api_version, model, api_key, payload):
    url = _gemini_generate_url(api_version, model, api_key)
    response = requests.post(url, json=payload, timeout=40)
    return response


def _call_openrouter(prompt, image_b64, mime_type):
    if not OPENROUTER_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    if OPENROUTER_REFERRER:
        headers["HTTP-Referer"] = OPENROUTER_REFERRER
    if OPENROUTER_TITLE:
        headers["X-Title"] = OPENROUTER_TITLE

    # Ask explicitly for JSON only.
    system_prompt = (
        "You are an OCR parser. Given an image of a prescription or medicine label, "
        "return ONLY a compact JSON object with keys: "
        "medicineName, dosage, startDate, endDate, time, mealType, mealRelation. "
        "Do not include code fences or extra text."
    )

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime_type};base64,{image_b64}"},
                    },
                ],
            }
        ],
        "temperature": 0.2,
        "max_tokens": 512,
        # JSON mode if supported by the model.
        "response_format": {"type": "json_object"},
    }

    try:
        res = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=45,
        )
    except requests.RequestException as exc:
        return {"error": f"OpenRouter request failed: {exc}"}

    if res.status_code != 200:
        return {"error": f"OpenRouter error {res.status_code}: {res.text}"}

    try:
        data = res.json()
    except Exception:
        return {"error": "OpenRouter response not valid JSON."}

    choices = data.get("choices") or []
    if not choices:
        return {"error": "OpenRouter returned no choices."}
    content = choices[0].get("message", {}).get("content", "")
    parsed = _extract_json_from_text(content)
    if not parsed or not isinstance(parsed, dict):
        return {"error": "Could not parse structured JSON from OpenRouter response."}

    return {
        "medicineName": str(parsed.get("medicineName", "")).strip(),
        "dosage": str(parsed.get("dosage", "")).strip(),
        "startDate": str(parsed.get("startDate", "")).strip(),
        "endDate": str(parsed.get("endDate", "")).strip(),
        "time": str(parsed.get("time", "")).strip(),
        "mealType": str(parsed.get("mealType", "")).strip(),
        "mealRelation": str(parsed.get("mealRelation", "")).strip(),
    }


def _normalized_mime_type(mime_type, filename=None):
    if mime_type and mime_type.startswith("image/"):
        return mime_type
    if filename:
        guessed, _ = mimetypes.guess_type(filename)
        if guessed and guessed.startswith("image/"):
            return guessed
    # Gemini expects a concrete image mime type; fall back to jpeg.
    return "image/jpeg"


def extract_medicine_details_from_image(image_bytes, mime_type):
    safe_mime = _normalized_mime_type(mime_type)

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

    # Try OpenRouter first if configured. If it returns anything (ok or error),
    # stop and return that result to avoid falling back to Gemini quotas.
    if OPENROUTER_API_KEY:
        or_result = _call_openrouter(prompt, image_b64, safe_mime)
        if or_result:
            return or_result

    if not GEMINI_API_KEY:
        return {"error": "GEMINI_API_KEY is not configured on backend."}

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt},
                    {
                        "inline_data": {
                            "mime_type": safe_mime,
                            "data": image_b64,
                        }
                    },
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 512,
            # responseMimeType is only supported on v1; for v1beta we strip it below.
            "responseMimeType": "application/json",
        },
    }

    # Try configured API version first, then fallback to the other stable option.
    api_versions = [GEMINI_API_VERSION]
    if GEMINI_API_VERSION != "v1":
        api_versions.append("v1")
    # v1beta last resort to avoid incompatible behaviors unless nothing else works.
    if GEMINI_API_VERSION != "v1beta":
        api_versions.append("v1beta")

    last_error = None
    for api_version in api_versions:
        available = _try_list_models(api_version, GEMINI_API_KEY)
        model_to_use = _pick_fallback_model(available, GEMINI_MODEL)

        # Build payload per version: drop responseMimeType for v1beta to avoid API errors,
        # but keep it for v1 to encourage structured JSON.
        request_payload = dict(payload)
        if api_version == "v1beta":
            gen_cfg = dict(request_payload.get("generationConfig") or {})
            gen_cfg.pop("responseMimeType", None)
            request_payload["generationConfig"] = gen_cfg

        try:
            response = _post_gemini_generate(api_version, model_to_use, GEMINI_API_KEY, request_payload)
        except requests.RequestException as exc:
            last_error = f"Gemini request failed ({api_version}/{model_to_use}): {exc}"
            continue

        if response.status_code != 200:
            # Try next api_version/model combo. Keep the most recent error for debugging.
            last_error = (
                f"Gemini API error {response.status_code} "
                f"({api_version}/{model_to_use}): {response.text}"
            )
            continue

        try:
            data = response.json()
        except Exception:
            last_error = f"Gemini response was not valid JSON ({api_version}/{model_to_use})."
            continue

        candidates = data.get("candidates", [])
        if not candidates:
            last_error = f"No response candidates from Gemini ({api_version}/{model_to_use})."
            continue

        parts = candidates[0].get("content", {}).get("parts", [])
        text_part = ""
        for part in parts:
            if "text" in part:
                text_part += part["text"]

        parsed = _extract_json_from_text(text_part)
        if not parsed or not isinstance(parsed, dict):
            last_error = (
                "Could not parse structured JSON from Gemini response "
                f"({api_version}/{model_to_use})."
            )
            continue

        return {
            "medicineName": str(parsed.get("medicineName", "")).strip(),
            "dosage": str(parsed.get("dosage", "")).strip(),
            "startDate": str(parsed.get("startDate", "")).strip(),
            "endDate": str(parsed.get("endDate", "")).strip(),
            "time": str(parsed.get("time", "")).strip(),
            "mealType": str(parsed.get("mealType", "")).strip(),
            "mealRelation": str(parsed.get("mealRelation", "")).strip(),
        }

    return {
        "error": last_error
        or "Gemini OCR failed. Check GEMINI_API_KEY/GEMINI_MODEL/GEMINI_API_VERSION."
    }
