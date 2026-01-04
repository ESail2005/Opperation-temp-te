import csv
import json
import ssl
import time
import urllib.parse
import urllib.request
from pathlib import Path

INPUT_CSV = Path("posts.csv")
OUTPUT_CSV = INPUT_CSV  # in-place update
MAX_RETRIES = 3
SLEEP_SECONDS = 0.08  # small delay between calls to avoid throttling


def reverse_geocode(lon, lat, ctx):
    params = {
        "f": "json",
        "location": f"{lon},{lat}",
        "langCode": "fr",
        "outSR": 4326,
    }
    url = (
        "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?"
        + urllib.parse.urlencode(params)
    )
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(url, context=ctx, timeout=10) as resp:
                data = json.load(resp)
            addr = data.get("address") or {}
            label = (
                addr.get("LongLabel")
                or addr.get("Address")
                or addr.get("Match_addr")
                or ""
            )
            return label
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"Reverse geocode failed for {lon},{lat}: {e}")
                return ""
            time.sleep(0.5 * attempt)
    return ""


def main():
    ctx = ssl._create_unverified_context()
    rows = list(csv.DictReader(INPUT_CSV.open()))
    header = rows[0].keys()
    updated = 0
    skipped = 0

    for row in rows:
        rue = row.get("RUE_REVERSE", "").strip()
        lon = row.get("LON_WGS84")
        lat = row.get("LAT_WGS84")

        # Only update when missing or marked MISSING_RUE
        if rue and rue.upper() != "MISSING_RUE":
            skipped += 1
            continue
        try:
            lon_f = float(lon) if lon not in (None, "", "None") else None
            lat_f = float(lat) if lat not in (None, "", "None") else None
        except ValueError:
            lon_f = lat_f = None
        if lon_f is None or lat_f is None:
            continue

        label = reverse_geocode(lon_f, lat_f, ctx)
        if label:
            row["RUE_REVERSE"] = label
            updated += 1
        time.sleep(SLEEP_SECONDS)

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Updated {updated} rows; skipped existing {skipped}.")


if __name__ == "__main__":
    main()
