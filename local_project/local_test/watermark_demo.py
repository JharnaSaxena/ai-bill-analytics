DELAY_SECONDS = 30

events = [
    {"id": "e1", "event_ts": "10:00:00"},
    {"id": "e2", "event_ts": "10:00:30"},
    {"id": "e3", "event_ts": "10:00:50"},
    {"id": "e4", "event_ts": "10:00:05"},
    {"id": "e5", "event_ts": "10:00:30"},
    {"id": "e6", "event_ts": "10:00:15"},
    {"id": "e7", "event_ts": "10:00:20"},
    {"id": "e8", "event_ts": "10:01:10"},
    {"id": "e9", "event_ts": "10:00:25"},
]

def to_seconds(t_str):
    h, m, s = map(int, t_str.split(":"))
    return h * 3600 + m * 60 + s

def fmt(seconds):
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

print(f"Watermark delay = {DELAY_SECONDS} seconds")
print("=" * 78)
print(f"{'Event':<6} {'event_ts':<10} {'max_seen':<10} {'watermark':<10} Decision")
print("-" * 78)

max_seen = None
accepted = 0
rejected = 0

for ev in events:
    ev_sec = to_seconds(ev["event_ts"])
    if max_seen is None or ev_sec > max_seen:
        max_seen = ev_sec
    watermark = max_seen - DELAY_SECONDS
    if ev_sec < watermark:
        decision = "REJECTED (too late)"
        rejected += 1
    else:
        decision = "ACCEPTED"
        accepted += 1
    print(f"{ev['id']:<6} {ev['event_ts']:<10} {fmt(max_seen):<10} {fmt(watermark):<10} {decision}")

print("=" * 78)
print(f"Summary: {accepted} accepted, {rejected} rejected")