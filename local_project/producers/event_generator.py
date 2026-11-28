import csv, os, random, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

STATUSES = ["success", "success", "success", "success", "rate_limited", "error"]
APPS     = ["chatbot", "rag-search", "summarizer", "code-assistant"]

# Virtual clock state (only used when SEQUENTIAL_CLOCK=true)
_CLOCK = {"ts": None}


def load_prices(path: Path):
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise SystemExit(f"empty price CSV: {path}")
    return rows


def _next_virtual_ts(real_now: datetime) -> datetime:
    """
    Sequential clock: advances 1 second per call, starting at today 09:00 UTC.
    Wraps to next day 09:00 when hitting 17:00.
    This makes the stream see events in order → watermark advances smoothly
    → every 1-minute window closes properly → dense speed output.
    """
    if _CLOCK["ts"] is None:
        _CLOCK["ts"] = datetime(real_now.year, real_now.month, real_now.day,
                                9, 0, 0, tzinfo=timezone.utc)
    else:
        _CLOCK["ts"] += timedelta(seconds=1)
        if _CLOCK["ts"].hour >= 17:
            nd = _CLOCK["ts"].date() + timedelta(days=1)
            _CLOCK["ts"] = datetime(nd.year, nd.month, nd.day,
                                    9, 0, 0, tzinfo=timezone.utc)
    return _CLOCK["ts"]


def make_event(valid_rows, late_rate=0.02, bad_rate=0.005):
    row = random.choice(valid_rows)
    now = datetime.now(timezone.utc)

    # Choose base timestamp by mode:
    # 1. SEQUENTIAL_CLOCK=true → virtual advancing clock (best for demo)
    # 2. Otherwise, if outside 09-17 UTC → random back-date within that window
    # 3. Otherwise → real now
    if os.getenv("SEQUENTIAL_CLOCK", "false").lower() == "true":
        base_ts = _next_virtual_ts(now)
    elif now.hour < 9 or now.hour >= 17:
        today = now.date()
        base_ts = datetime(today.year, today.month, today.day,
                           random.randint(9, 16),
                           random.randint(0, 59),
                           random.randint(0, 59),
                           tzinfo=timezone.utc)
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
        if kind == "missing_id":
            ev.pop("event_id", None)
        elif kind == "negative_tokens":
            ev["tokens_in"] = -abs(ev["tokens_in"])
        else:
            ev["model"] = "totally-not-a-real-model"

    return ev
