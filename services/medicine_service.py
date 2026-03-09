import requests
from os import getenv

OPENFDA_BASE_URL = getenv("OPENFDA_BASE_URL", "https://api.fda.gov/drug/label.json")
OPENFDA_API_KEY = getenv("OPENFDA_API_KEY")

def _format_medicine_response(result):
    openfda = result.get("openfda", {})
    return {
        "brand": openfda.get("brand_name", ["Unknown"])[0],
        "generic": openfda.get("generic_name", ["Unknown"])[0],
        "usage": result.get("indications_and_usage", ["Not available"])[0],
        "dosage": result.get("dosage_and_administration", ["Not available"])[0],
        "side_effects": result.get("adverse_reactions", ["Not available"])[0]
    }

def _barcode_candidates(raw_code):
    code = "".join(ch for ch in raw_code if ch.isdigit())
    candidates = []
    seen = set()

    def add(value):
        if not value or value in seen:
            return
        seen.add(value)
        candidates.append(value)

    if not code:
        return []

    # Always try raw numeric value first.
    add(code)

    # EAN-13 that starts with 0 often wraps a UPC-A payload.
    if len(code) == 13 and code.startswith("0"):
        add(code[1:])

    upc12 = None
    if len(code) == 12:
        upc12 = code
    elif len(code) == 13 and code.startswith("0"):
        upc12 = code[1:]

    # Common Rx UPC-A pattern:
    # 12-digit UPC => [number-system][10-digit NDC payload][check-digit]
    if upc12:
        ndc10 = upc12[1:11]
        add(ndc10)

        # 10-digit NDC can be one of 4-4-2, 5-3-2, 5-4-1.
        # Convert to 11-digit by zero-padding the correct segment.
        if len(ndc10) == 10:
            # 4-4-2 -> 5-4-2
            ndc11_a = f"0{ndc10[0:4]}{ndc10[4:8]}{ndc10[8:10]}"
            # 5-3-2 -> 5-4-2
            ndc11_b = f"{ndc10[0:5]}0{ndc10[5:8]}{ndc10[8:10]}"
            # 5-4-1 -> 5-4-2
            ndc11_c = f"{ndc10[0:5]}{ndc10[5:9]}0{ndc10[9:10]}"

            add(ndc11_a)
            add(ndc11_b)
            add(ndc11_c)

            # Hyphenated 10-digit representations.
            add(f"{ndc10[0:4]}-{ndc10[4:8]}-{ndc10[8:10]}")
            add(f"{ndc10[0:5]}-{ndc10[5:8]}-{ndc10[8:10]}")
            add(f"{ndc10[0:5]}-{ndc10[5:9]}-{ndc10[9:10]}")

            # Hyphenated 11-digit 5-4-2 representations.
            for ndc11 in (ndc11_a, ndc11_b, ndc11_c):
                add(f"{ndc11[0:5]}-{ndc11[5:9]}-{ndc11[9:11]}")

    # Also try 11-digit to hyphenated FDA format.
    if len(code) == 11:
        add(f"{code[0:5]}-{code[5:9]}-{code[9:11]}")

    return candidates

def get_medicine(name):
    base_url = OPENFDA_BASE_URL
    search_terms = [
        f'openfda.generic_name:"{name}"',
        f'openfda.brand_name:"{name}"',
        f'openfda.substance_name:"{name}"'
    ]

    data = {}
    for search in search_terms:
        try:
            params = {"search": search, "limit": 1}
            if OPENFDA_API_KEY:
                params["api_key"] = OPENFDA_API_KEY
            r = requests.get(base_url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if "results" in data and data["results"]:
                    break
        except requests.RequestException:
            continue

    if "results" not in data or not data["results"]:
        return {"message": "Medicine not found"}

    result = data["results"][0]
    return _format_medicine_response(result)

def get_medicine_by_barcode(barcode):
    if not barcode:
        return {"message": "Barcode is required"}

    base_url = OPENFDA_BASE_URL
    candidates = _barcode_candidates(barcode.strip())
    if not candidates:
        return {"message": "Invalid barcode"}

    search_terms = []
    for code in candidates:
        search_terms.extend([
            f'openfda.product_ndc:"{code}"',
            f'openfda.package_ndc:"{code}"',
            f'openfda.spl_id:"{code}"'
        ])

    data = {}
    for search in search_terms:
        try:
            params = {"search": search, "limit": 1}
            if OPENFDA_API_KEY:
                params["api_key"] = OPENFDA_API_KEY
            r = requests.get(base_url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if "results" in data and data["results"]:
                    break
        except requests.RequestException:
            continue

    if "results" not in data or not data["results"]:
        return {
            "message": (
                "Medicine not found for barcode in OpenFDA. "
                "OpenFDA barcode/NDC data is mostly US-market; many non-US barcodes are not present."
            )
        }

    result = data["results"][0]
    return _format_medicine_response(result)
