import csv, random, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

STATUSES = ["success", "success", "success", "success", "rate_limited", "error"]
APPS     = ["chatbot", "rag-search", "summarizer", "code-assistant"]

def load_prices(path: Path):
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise SystemExit(f"empty price CSV: {path}")
    return rows

def make_event(valid_rows, late_rate=0.02, bad_rate=0.005):
    row = random.choice(valid_rows)
    now = datetime.now(timezone.utc)

    # If current UTC hour is OUTSIDE 09:00-17:00, back-date to a random minute
    # inside today's slot window. This keeps batch (slot-only) fed with data
    # even when running at night / early morning.
    if now.hour < 9 or now.hour >= 17:
        today = now.date()
        random_hour   = random.randint(9, 16)   # 09:00 to 16:xx UTC
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        base_ts = datetime(
            today.year, today.month, today.day,
            random_hour, random_minute, random_second,
            tzinfo=timezone.utc
        )
    else:
        base_ts = now

    ev = {
        "event_id":   str(uuid.uuid4()),
        "request_id": "req_" + uuid.uuid4().hex[:16],
        "event_ts":   base_ts.isoformat(),
        "team_id":    row["team_id"],
        "app_id":     random.choice(APPS),
        "model":      row["model"],
        "tokens_in":  random.randint(50, 4000),
        "tokens_out": random.randint(50, 2000),
        "status":     random.choice(STATUSES),
    }

    if random.random() < late_rate:
        ev["event_ts"] = (base_ts - timedelta(seconds=random.randint(30, 300))).isoformat()

    if random.random() < bad_rate:
        kind = random.choice(["missing_id", "negative_tokens", "unknown_model"])
        if kind == "missing_id": ev.pop("event_id", None)
        elif kind == "negative_tokens": ev["tokens_in"] = -abs(ev["tokens_in"])
        else: ev["model"] = "totally-not-a-real-model"

    return ev