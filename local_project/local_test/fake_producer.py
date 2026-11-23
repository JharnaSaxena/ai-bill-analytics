import json, random, time, uuid
from datetime import datetime, timezone
from pathlib import Path

INPUT = Path(__file__).resolve().parent / "input"
INPUT.mkdir(exist_ok=True)

TEAMS    = ["team-a","team-b","team-c"]
MODELS   = ["gpt-4o","gpt-4o-mini","claude-sonnet-4"]
STATUSES = ["success","success","success","rate_limited","error"]

i = 0
while True:
    i += 1
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = INPUT / f"batch_{i:04d}_{ts}.jsonl"
    with path.open("w") as f:
        for _ in range(20):
            ev = {
                "event_id":   str(uuid.uuid4()),
                "request_id": "req_" + uuid.uuid4().hex[:16],
                "event_ts":   datetime.now(timezone.utc).isoformat(),
                "team_id":    random.choice(TEAMS),
                "app_id":     "chatbot",
                "model":      random.choice(MODELS),
                "tokens_in":  random.randint(50, 4000),
                "tokens_out": random.randint(50, 2000),
                "status":     random.choice(STATUSES),
            }
            f.write(json.dumps(ev) + "\n")
    print(f"[fake-producer] wrote {path.name} (20 events)", flush=True)
    time.sleep(5)