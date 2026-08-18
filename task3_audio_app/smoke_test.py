"""
Smoke tests for Task 3 API.
Run from task3_audio_app/ directory while the server is running on :8000.
"""
import wave, struct, math, os, requests, json

BASE = "http://127.0.0.1:8000"

def make_wav_bytes():
    import io
    buf = io.BytesIO()
    with wave.open(buf, 'w') as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(44100)
        samples = [int(32767 * math.sin(2 * math.pi * 440 * t / 44100)) for t in range(88200)]
        wf.writeframes(struct.pack('<' + 'h' * len(samples), *samples))
    buf.seek(0)
    return buf.read()

wav_bytes = make_wav_bytes()
print(f"Generated test WAV: {len(wav_bytes)} bytes")

print("\n[T1] GET /stats")
r = requests.get(f"{BASE}/stats")
print(f"  Status: {r.status_code}  Body: {r.text}")
assert r.status_code == 200
stats_before = r.json()

print("\n[T2] GET /submissions")
r = requests.get(f"{BASE}/submissions")
print(f"  Status: {r.status_code}  Count: {len(r.json())} rows")
assert r.status_code == 200

print("\n[T3] POST /submit -- existing person phone +919000000254")
r = requests.post(
    f"{BASE}/submit",
    data={"name": "Tanvi Gupta", "phone": "+919000000254"},
    files={"audio": ("test.wav", wav_bytes, "audio/wav")},
)
print(f"  Status: {r.status_code}")
body = r.json()
print(f"  Response: {json.dumps(body, indent=2)}")
assert r.status_code == 200, f"Expected 200, got {r.status_code}: {body}"
assert body["person_id"] == 1, f"Expected person_id=1 (Tanvi Gupta in Task1 DB), got {body['person_id']}"
print("  [OK] Linked to existing person_id=1 (Tanvi Gupta)")

print("\n[T4] POST /submit -- unknown phone 9999999999 (new person should be created)")
r = requests.post(
    f"{BASE}/submit",
    data={"name": "Completely New Person", "phone": "9999999999"},
    files={"audio": ("test.wav", wav_bytes, "audio/wav")},
)
body2 = r.json()
print(f"  Status: {r.status_code}  person_id={body2.get('person_id')}")
assert r.status_code == 200
assert body2["person_id"] > 61, f"Expected a new person_id > 61 (Task1 had 61), got {body2['person_id']}"
print(f"  [OK] New person_id={body2['person_id']} created for walk-in")

print("\n[T5] POST /submit -- invalid phone '123' should return 422")
r = requests.post(
    f"{BASE}/submit",
    data={"name": "Bad Phone", "phone": "123"},
    files={"audio": ("test.wav", wav_bytes, "audio/wav")},
)
print(f"  Status: {r.status_code}")
assert r.status_code == 422, f"Expected 422, got {r.status_code}"
print("  [OK] 422 returned for invalid phone")

print("\n[T6] GET /stats -- should show 2 new submissions")
r = requests.get(f"{BASE}/stats")
stats_after = r.json()
print(f"  Stats: {json.dumps(stats_after, indent=2)}")
new_count = stats_after["total_submissions"] - stats_before["total_submissions"]
assert new_count == 2, f"Expected 2 new submissions, got {new_count}"
print(f"  [OK] total_submissions increased by 2")

print("\n[T7] GET /submissions -- check audio metadata fields")
r = requests.get(f"{BASE}/submissions")
rows = r.json()
latest = rows[0]
print(f"  Latest row: name={latest['name']}  duration={latest['duration_sec']}s  "
      f"sample_rate={latest['sample_rate_hz']}Hz  "
      f"loudness={latest['loudness_dbfs']} dBFS  "
      f"noise_flag={latest['noise_flag']}")

print("\n" + "="*50)
print("ALL SMOKE TESTS PASSED [OK]")
