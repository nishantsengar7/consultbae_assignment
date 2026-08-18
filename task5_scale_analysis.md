# Task 5 — Scaling Analysis: Launching to 5,000 Gig Workers

**Scenario:** Deploying the ConsultBae Audio Collection App to 5,000 gig workers over a 48-hour weekend recruitment campaign (~100–250 concurrent active users during peak hours).

---

## 1. What Breaks First? (The Critical Bottlenecks)

### A. CPU & Thread Starvation via Synchronous FFmpeg Decoding
* **The Failure:** Currently, `POST /submit` synchronously invokes an `ffmpeg` subprocess within the request-response cycle to decode and compute audio metrics (`dBFS`, sample rate, noise crest factor).
* **The Impact:** Spawning 30–50 concurrent FFmpeg processes will immediately max out CPU cores (100% utilization), blocking the Python event loop. Client requests will experience latency spikes from 300ms to >30 seconds, causing gateway timeouts (HTTP 504) and dropped connections.

### B. SQLite Write-Lock Contention (`database is locked`)
* **The Failure:** SQLite operates with file-level locking on write transactions (`INSERT INTO persons`, `INSERT INTO audio_submissions`).
* **The Impact:** When dozens of workers submit simultaneously, concurrent write transactions will queue up. Once the lock timeout (default 5s) expires, SQLite will throw `OperationalError: database is locked`, causing unhandled 500 Internal Server Errors for applicants.

### C. Race Conditions & Duplicate Candidates
* **The Failure:** `resolve_person()` performs a check-then-act query (`SELECT person_id` followed by `INSERT INTO persons`).
* **The Impact:** When a candidate double-taps the submit button on mobile or two submissions arrive concurrently with the same phone number, both threads see no existing record and execute parallel `INSERT` statements, producing duplicate candidate profiles.

### D. Mobile Upload Drops on Unstable 4G/3G Networks
* **The Failure:** Audio files are streamed as monolithic multipart HTTP POSTs directly to the backend application server.
* **The Impact:** Gig workers recording on mobile in low-bandwidth areas will suffer frequent socket resets. Without resumable uploads or direct cloud storage streaming, any mid-upload packet loss forces the candidate to re-record from scratch.

---

## 2. What I Would Change Before Launch (Production Blueprint)

```
[Mobile / Web Client] 
         │
         ├── 1. Request Presigned Upload URL ──► [FastAPI Gateway / Auth]
         │                                               │
         ├── 2. Direct Chunked Upload ──────────► [Cloud Object Storage (S3/GCS)]
         │                                               │ (S3 Event Notification)
         └── 3. Submit Metadata & Phone ────────►        ▼
                                                  [Message Queue (SQS / Redis)]
                                                         │
                                                         ▼
                                                  [Async Worker Pool (Celery / Lambda)]
                                                         │ (FFmpeg Analysis)
                                                         ▼
                                                  [Managed Relational DB (PostgreSQL)]
```

### 1. Decouple Ingestion from Audio Processing (Async Worker Queue)
* **Architecture Change:** Make the submission endpoint lightweight and non-blocking.
* **Flow:** `POST /submit` immediately writes the submission payload with status `pending_processing`, pushes a job into a message queue (**Redis / AWS SQS**), and returns `202 Accepted` to the user in under 50ms.
* **Worker Tier:** A background worker pool (**Celery** or **AWS Lambda**) consumes the queue, executes FFmpeg decoding and metadata extraction asynchronously, and updates the database record upon completion.

### 2. Direct-to-Storage Uploads via Presigned URLs
* **Architecture Change:** Offload all binary payload traffic from the application server.
* **Flow:** The client requests a presigned S3/GCS upload URL (`GET /upload-url`), uploads the audio directly to object storage via HTTP PUT with chunking/retry support, and then posts the file key and candidate phone number to the API.
* **Benefit:** Web servers handle only lightweight JSON requests, cutting server bandwidth and memory usage by 95%.

### 3. Migrate from SQLite to Managed PostgreSQL with Atomic Upserts
* **Architecture Change:** Replace local SQLite with a managed relational database (**AWS RDS PostgreSQL** / **Supabase**).
* **Concurrency & Safety:**
  - Add a `UNIQUE` constraint on `persons.canonical_phone`.
  - Use atomic SQL upserts:
    ```sql
    INSERT INTO persons (canonical_name, canonical_phone, sources)
    VALUES ($1, $2, 'task3_audio')
    ON CONFLICT (canonical_phone) DO UPDATE SET source_count = source_count + 1
    RETURNING person_id;
    ```
  - This mathematically eliminates race-condition duplicate profiles under high concurrency.

### 4. Client-Side Resilience & Rate Limiting
* **Debouncing & Idempotency:** Disable the submit button on first tap and attach a client-generated UUID `Idempotency-Key` header to prevent double-charging or duplicate submissions.
* **Rate Limiting:** Implement token-bucket rate limiting (e.g. max 5 submissions per IP/phone per hour) to protect against script attacks or accidental retry loops.
* **Audio Format Constraints:** Enforce client-side duration caps (e.g., max 60 seconds) and compress recorded audio into Opus WebM before upload to keep payload sizes strictly under 1 MB.

---

## 3. Cost & Storage Breakdown (5,000 Submissions)

| Component | Weekend Volume / Spec | Estimated Cost |
| :--- | :--- | :--- |
| **Object Storage (AWS S3)** | 5,000 clips $\times$ 500 KB avg $\approx$ **2.5 GB** | **$0.06 / month** (negligible) |
| **Data Transfer In** | 2.5 GB ingress to S3 | **Free** ($0.00) |
| **Compute (API Tier)** | 2x small containers (AWS Fargate / ECS, 0.5 vCPU, 1 GB RAM) | **$3.50** for 48 hours |
| **Async Processing** | 5,000 Lambda invocations (2s execution @ 512 MB) | **$0.02** (well within free tier) |
| **Database (PostgreSQL)** | AWS RDS `db.t4g.micro` burstable instance | **$1.20** for 48 hours |
| **Total Campaign Infrastructure Cost** | **5,000 Completed Candidate Profiles** | **< $5.00 total** |

---

## Summary of Changes

| Domain | Prototype (Current) | Production-Ready Architecture |
| :--- | :--- | :--- |
| **Audio Processing** | Synchronous FFmpeg subprocess on request thread | Asynchronous background workers (SQS + Celery / Lambda) |
| **Database** | SQLite local file with table write-locking | Managed PostgreSQL with connection pooling & atomic `ON CONFLICT` |
| **Storage** | Local disk `uploads/` directory on server instance | S3 / GCS cloud object storage with presigned direct uploads |
| **Reliability** | Monolithic HTTP POST; fails on mobile packet drop | Direct S3 multi-part uploads with retry capability |
| **Duplicates** | Check-then-act query prone to concurrency race conditions | Database-level unique constraint with atomic upsert |
