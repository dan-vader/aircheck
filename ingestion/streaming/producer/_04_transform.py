def pivot_record(rec: dict) -> dict:
    event = {
        "id": rec.get("id"),
        "timestamp": rec.get("timestamp"),
        "sensor_id": rec.get("sensor", {}).get("id"),
        "sensor_type": rec.get("sensor", {}).get("sensor_type", {}).get("name"),
        "location_id": rec.get("location", {}).get("id"),
        "latitude": rec.get("location", {}).get("latitude"),
        "longitude": rec.get("location", {}).get("longitude"),
        "country": rec.get("location", {}).get("country"),
    }
    for v in rec.get("sensordatavalues", []):
        value_type = v.get("value_type")
        if value_type:
            event[value_type] = v.get("value")
    return event