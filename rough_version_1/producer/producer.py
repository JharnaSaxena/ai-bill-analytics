import csv
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC           = os.getenv("TOPIC", "llm-usage-events")
PROVIDER_TOPIC  = "llm-provider-reports"
DATA_DIR        = Path(os.getenv("DATA_DIR", "/opt/data"))
PRICE_CSV       = Path(os.getenv("PRICE_CSV", str(DATA_DIR / "price" / "price_budget.csv")))

RATE_PER_SEC           = float(os.getenv("RATE_PER_SEC", "50"))
RESPECT_BUSINESS_HOURS = os.getenv("RESPECT_BUSINESS_HOURS", "false").lower() in ("1","true","yes")
OFFICE_TZ              = ZoneInfo(os.getenv("OFFICE_TZ", "Asia/Kolkata"))
DURATION_SEC           = int(os.getenv("DURATION_SEC", "0"))
DUPLICATE_RATE         = float(os.getenv("DUPLICATE_RATE", "0.02"))
LATE_RATE              = float(os.getenv("LATE_RATE", "0.02"))
BAD_RATE               = float(os.getenv("BAD_RATE", "0.005"))
PROVIDER_RATE          = float(os.getenv("PROVIDER_RATE", "0.25"))

STATUSES = ["success","success","success","success","rate_limited","error"]
APPS     = ["chatbot","rag-search","summarizer","code-assistant"]


def load_prices(path):
    rows = []
    with path.open(newline="") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    if not rows:
        raise SystemExit(f"empty price CSV: {path}")
    return rows


def hour_factor():
    now = datetime.now(OFFICE_TZ)
    if now.weekday() >= 5:
        return 0.02
    h = now.hour
    if 9 <= h < 17: return 1.0
    if 8 <= h < 9 or 17 <= h < 18: return 0.5
    if 6 <= h < 8 or 18 <= h < 22: return 0.15
    return 0.03


def make_event(valid_rows):
    row = random.choice(valid_rows)
    now = datetime.now(timezone.utc)
    event = {
        "event_id":   str(uuid.uuid4()),
        "request_id": "req_" + uuid.uuid4().hex[:16],
        "event_ts":   now.isoformat(),
        "team_id":    row["team_id"],
        "app_id":     random.choice(APPS),
        "model":      row["model"],
        "tokens_in":  random.randint(50, 4000),
        "tokens_out": random.randint(50, 2000),
        "status":     random.choice(STATUSES),
    }
    late = False
    if random.random() < LATE_RATE:
        event["event_ts"] = (now - timedelta(seconds=random.randint(30, 300))).isoformat()
        late = True
    if random.random() < BAD_RATE:
        kind = random.choice(["missing_id","negative_tokens","unknown_model"])
        if kind == "missing_id":
            event.pop("event_id", None)
        elif kind == "negative_tokens":
            event["tokens_in"] = -abs(event["tokens_in"])
        else:
            event["model"] = "totally-not-a-real-model"
    return event, late


class MinuteWriter:
    FLUSH_EVERY = 10
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.key = None
        self.fh  = None
        self.buf = 0
    def write(self, payload, ts_utc):
        key = (ts_utc.year, ts_utc.month, ts_utc.day, ts_utc.hour, ts_utc.minute)
        if key != self.key:
            self._rotate(key)
        self.fh.write(json.dumps(payload) + "\n")
        self.buf += 1
        if self.buf >= self.FLUSH_EVERY:
            self.fh.flush()
            self.buf = 0
    def _rotate(self, key):
        if self.fh: self.fh.close()
        y,m,d,h,mn = key
        path = (self.base_dir / f"year={y:04d}" / f"month={m:02d}"
                / f"day={d:02d}" / f"hour={h:02d}" / f"{mn:02d}.jsonl")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.fh  = path.open("a", encoding="utf-8")
        self.key = key
        self.buf = 0
    def close(self):
        if self.fh: self.fh.close(); self.fh = None


def main():
    valid_rows = load_prices(PRICE_CSV)
    (DATA_DIR / "raw").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "raw_provider").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "dead-letter").mkdir(parents=True, exist_ok=True)

    usage_writer    = MinuteWriter(DATA_DIR / "raw")
    provider_writer = MinuteWriter(DATA_DIR / "raw_provider")

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all", retries=3, linger_ms=5)
    provider_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all", retries=3, linger_ms=5)

    print(f"[producer] kafka={KAFKA_BOOTSTRAP} topic={TOPIC} provider_topic={PROVIDER_TOPIC} "
          f"rate={RATE_PER_SEC}/s office_hours={RESPECT_BUSINESS_HOURS} tz={OFFICE_TZ} "
          f"dup={DUPLICATE_RATE} late={LATE_RATE} bad={BAD_RATE} provider_rate={PROVIDER_RATE} "
          f"data_dir={DATA_DIR}", flush=True)

    stop = {"flag": False}
    def _stop(*_):
        stop["flag"] = True
        print("[producer] stopping...", flush=True)
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    start = time.time()
    sent = dup = late_count = provider_count = 0
    next_recalc = 0.0
    current_rate = RATE_PER_SEC

    while not stop["flag"]:
        if DURATION_SEC and (time.time() - start) >= DURATION_SEC:
            break

        now_mono = time.monotonic()
        if now_mono >= next_recalc:
            factor = hour_factor() if RESPECT_BUSINESS_HOURS else 1.0
            current_rate = max(0.2, RATE_PER_SEC * factor)
            next_recalc  = now_mono + 1.0

        ev, was_late = make_event(valid_rows)
        if was_late: late_count += 1

        ts_utc = datetime.now(timezone.utc)
        usage_writer.write(ev, ts_utc)
        producer.send(TOPIC, ev)

        if random.random() < DUPLICATE_RATE:
            producer.send(TOPIC, ev)
            usage_writer.write(ev, ts_utc)
            dup += 1

        if random.random() < PROVIDER_RATE and ev.get("event_id"):
            billed_total = int((ev["tokens_in"] + ev["tokens_out"]) * random.uniform(1.02, 1.08))
            report = {
                "request_id":    ev["request_id"],
                "billed_tokens": billed_total,
                "status":        ev["status"],
                "reported_at":   datetime.now(timezone.utc).isoformat(),
            }
            provider_producer.send(PROVIDER_TOPIC, report)
            provider_writer.write(report, datetime.now(timezone.utc))
            provider_count += 1

        sent += 1

        if sent % 100 == 0:
            producer.flush(); provider_producer.flush()
            elapsed = time.time() - start
            print(f"[producer] sent={sent} dup={dup} late={late_count} provider={provider_count} "
                  f"rate_now={current_rate:.1f}/s avg={sent/elapsed:.1f}/s elapsed={elapsed:.1f}s",
                  flush=True)

        time.sleep(1.0 / current_rate)

    usage_writer.close(); provider_writer.close()
    producer.flush(); provider_producer.flush()
    producer.close(timeout=10); provider_producer.close(timeout=10)
    print(f"[producer] done. total={sent} dup={dup} late={late_count} provider={provider_count}",
          flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[producer] FATAL: {e}", file=sys.stderr, flush=True)
        raise