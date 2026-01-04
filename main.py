import csv
import datetime as dt
import json
import math
import ssl
import urllib.parse
import urllib.request
import time
from typing import List, Dict, Any

LAYER_URL = "https://services1.arcgis.com/4GCvRJNX6LNyFVQ0/ArcGIS/rest/services/CI_OPERATION_DENEIGEMENT/FeatureServer/0/query"
GEOM_URL = "https://utility.arcgisonline.com/ArcGIS/rest/services/Geometry/GeometryServer/project"
OUT_FIELDS = ["STATION_NO", "STATUT", "DATE_MAJ", "STATIONNEMENT", "OBJECTID"]
PAGE_SIZE = 500
OUTPUT_CSV = "posts.csv"
REVERSE_URL = "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode"


def fetch_page(offset: int, limit: int) -> Dict[str, Any]:
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": ",".join(OUT_FIELDS),
        "resultOffset": offset,
        "resultRecordCount": limit,
        "returnGeometry": "true",
    }
    url = f"{LAYER_URL}?{urllib.parse.urlencode(params)}"
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(url, context=ctx) as resp:
        return json.load(resp)


def project_points(points: List[Dict[str, float]]) -> List[Dict[str, float]]:
    """Project a list of {x,y} in SR 32187 to WGS84 (4326)."""
    if not points:
        return []
    geometries = json.dumps({"geometryType": "esriGeometryPoint", "geometries": points})
    params = {
        "f": "json",
        "inSR": 32187,
        "outSR": 4326,
        "geometries": geometries,
    }
    data_bytes = urllib.parse.urlencode(params).encode()
    ctx = ssl._create_unverified_context()
    req = urllib.request.Request(GEOM_URL, data=data_bytes, method="POST")
    with urllib.request.urlopen(req, context=ctx) as resp:
        data = json.load(resp)
    return data.get("geometries", [])


def ms_to_iso(ms):
    if ms is None:
        return ""
    return dt.datetime.utcfromtimestamp(ms / 1000).isoformat()


def reverse_geocode(lon: float, lat: float, cache: Dict[str, str]) -> str:
    if lon is None or lat is None:
        return ""
    key = f"{lon:.6f},{lat:.6f}"
    if key in cache:
        return cache[key]
    params = {
        "f": "json",
        "location": f"{lon},{lat}",
        "langCode": "fr",
        "outSR": 4326,
    }
    url = f"{REVERSE_URL}?{urllib.parse.urlencode(params)}"
    ctx = ssl._create_unverified_context()
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
        cache[key] = label
        return label
    except Exception:
        cache[key] = ""
        return ""


def load_existing() -> Dict[str, Dict[str, str]]:
    cache = {}
    try:
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cache[row.get("OBJECTID", "")] = row
    except FileNotFoundError:
        pass
    return cache


def main():
    existing_rows = load_existing()
    all_features = []
    offset = 0

    while True:
        data = fetch_page(offset, PAGE_SIZE)
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        offset += len(features)
        if len(features) < PAGE_SIZE:
            break

    # Project geometries to WGS84 in batches
    points = [
        {"x": f.get("geometry", {}).get("x"), "y": f.get("geometry", {}).get("y")}
        for f in all_features
    ]
    projected = []
    batch_size = 200
    for i in range(0, len(points), batch_size):
        projected.extend(project_points(points[i : i + batch_size]))

    reverse_cache: Dict[str, str] = {}
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "STATION_NO",
                "STATUT",
                "DATE_MAJ_ISO",
                "STATIONNEMENT",
                "OBJECTID",
                "X_32187",
                "Y_32187",
                "LON_WGS84",
                "LAT_WGS84",
                "RUE_REVERSE",
            ]
        )
        for idx, feat in enumerate(all_features):
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry", {}) or {}
            objectid = str(attrs.get("OBJECTID", ""))
            existing = existing_rows.get(objectid, {})
            if existing:
                lon = existing.get("LON_WGS84") or ""
                lat = existing.get("LAT_WGS84") or ""
                street_label = existing.get("RUE_REVERSE") or ""
            else:
                wgs84 = projected[idx] if idx < len(projected) else {}
                lon = wgs84.get("x")
                lat = wgs84.get("y")
                street_label = reverse_geocode(lon, lat, reverse_cache)
            writer.writerow(
                [
                    attrs.get("STATION_NO", ""),
                    attrs.get("STATUT", ""),
                    ms_to_iso(attrs.get("DATE_MAJ")),
                    attrs.get("STATIONNEMENT", ""),
                    objectid,
                    geom.get("x", ""),
                    geom.get("y", ""),
                    lon if lon is not None else "",
                    lat if lat is not None else "",
                    street_label,
                ]
            )

    print(f"Wrote {len(all_features)} postes to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
