import json, os, random, signal, time
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaProducer
from event_generator import load_prices, make_event

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC           = os.getenv("TOPIC", "llm-usage-events")
DATA_DIR        = Path(os.getenv("DATA_DIR", "/opt/data"))
PRICE_CSV       = Path(os.getenv("PRICE_CSV", str(DATA_DIR / "price" / "price_budget.csv")))

RATE_PER_SEC   = float(os.getenv("RATE_PER_SEC", "50"))
DURATION_SEC   = int(os.getenv("DURATION_SEC", "0"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.02"))
LATE_RATE      = float(os.getenv("LATE_RATE", "0.02"))
BAD_RATE       = float(os.getenv("BAD_RATE", "0.005"))


class MinuteWriter:
    FLUSH_EVERY = 10
    def __init__(self, base):
        self.base = base; self.key = None; self.fh = None; self.buf = 0
    def write(self, ev, ts):
        key = (ts.year, ts.month, ts.day, ts.hour, ts.minute)
        if key != self.key: self._rotate(key)
        self.fh.write(json.dumps(ev) + "\n"); self.buf += 1
        if self.buf >= self.FLUSH_EVERY: self.fh.flush(); self.buf = 0
    def _rotate(self, key):
        if self.fh: self.fh.close()
        y,m,d,h,mn = key
        p = (self.base / f"year={y:04d}" / f"month={m:02d}"
             / f"day={d:02d}" / f"hour={h:02d}" / f"{mn:02d}.jsonl")
        p.parent.mkdir(parents=True, exist_ok=True)
        self.fh = p.open("a", encoding="utf-8"); self.key = key; self.buf = 0
    def close(self):
        if self.fh: self.fh.close()


def main():
    rows = load_prices(PRICE_CSV)
    (DATA_DIR / "raw").mkdir(parents=True, exist_ok=True)
    writer = MinuteWriter(DATA_DIR / "raw")
    kafka = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all", retries=3, linger_ms=5)

    print(f"[producer] kafka={KAFKA_BOOTSTRAP} topic={TOPIC}", flush=True)
    print(f"[producer] disk={DATA_DIR}/raw rate={RATE_PER_SEC}/s", flush=True)
    print(f"[producer] fan-out: each event -> disk AND Kafka (same event_id)", flush=True)

    stop = {"flag": False}
    def _stop(*_): stop["flag"] = True; print("[producer] stopping...", flush=True)
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    start = time.time(); sent = 0; dups = 0
    while not stop["flag"]:
        if DURATION_SEC and time.time() - start >= DURATION_SEC:
            break
        ev = make_event(rows, late_rate=LATE_RATE, bad_rate=BAD_RATE)
        ts = datetime.now(timezone.utc)
        writer.write(ev, ts)
        kafka.send(TOPIC, ev)
        if random.random() < DUPLICATE_RATE:
            writer.write(ev, ts)
            kafka.send(TOPIC, ev)
            dups += 1
        sent += 1
        if sent % 100 == 0:
            kafka.flush(); writer.fh.flush()
            elapsed = time.time() - start
            print(f"[producer] sent={sent} dup={dups} avg={sent/elapsed:.1f}/s", flush=True)
        time.sleep(1.0 / RATE_PER_SEC)

    writer.close(); kafka.flush(); kafka.close(timeout=10)
    print(f"[producer] done. total={sent} dup={dups}", flush=True)


if __name__ == "__main__":
    main()