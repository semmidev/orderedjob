# 🎯 Roadmap & Future Engineering Improvements (`orderedjob`)

Dokumen ini berisi daftar analisis teknis mendalam mengenai aspek-aspek engineering, fitur, resiliensi terdistribusi, observabilitas, serta pengalaman pengembang (*Developer Experience*) yang **belum atau perlu di-cover** dalam pustaka `orderedjob` (di luar penambahan *driver* database baru).

Roadmap ini dirancang dari sudut pandang *Principal Software Architect / Senior Distributed Systems Engineer* untuk membawa `orderedjob` dari status *production-ready library* menjadi *enterprise-grade, high-throughput distributed job engine*.

---

## 📊 Ringkasan Progress Overall

- [x] **V1 Core Architecture** (Strict Order FIFO, Advisory Locks, Fencing Tokens, PG/MSSQL Drivers, Web UI Dashboard)
- [ ] **Phase 1**: Core Queue Architecture & Execution Mechanics
- [ ] **Phase 2**: Resilience, Fault Tolerance & DLQ Engineering
- [ ] **Phase 3**: Enterprise Observability & Telemetry (OpenTelemetry)
- [ ] **Phase 4**: Developer Experience, Testing Utilities & CLI Tooling
- [ ] **Phase 5**: Advanced Web UI Features & Security
- [ ] **Phase 6**: High-Performance Storage, Compression & Housekeeping
- [ ] **Phase 7**: Reliability Engineering & Chaos Testing Suite

---

## 1. ⚙️ Core Queue Architecture & Execution Mechanics

Aspek ini berfokus pada fleksibilitas eksekusi, pola penjadwalan, serta pencegahan masalah persaingan sumber daya (*resource starvation*).

### 1.1 Policy-driven Execution Modes
Saat ini `orderedjob` mengunci chain secara ketat jika job `N-1` gagal (*strict mode*). Di sistem produksi riil, tidak semua kasus bisnis memerlukan pemblokiran total.
- [ ] **Skip-on-Failure Policy**: Opsi konfigurasi per chain/engine untuk otomatis melanjutkan ke `N+1` jika job `N` mengalami kegagalan terminal (DLQ).
- [ ] **Dead-Letter-and-Continue Policy**: Mengkarantina job gagal ke DLQ tanpa menahan job urutan berikutnya.
- [ ] **Configurable Chain Ordering Strategy**: Mendukung strategi urutan dinamis per `job_type` atau per `tenant_id`.

### 1.2 Tenant & Chain-Level Fair Dispatching (Anti Noisy-Neighbor)
Saat ini worker mengambil job berdasarkan pola global poll/claim. Jika 1 tenant meng-enqueue 1.000.000 job, tenant lain bisa mengalami kelaparan (*starvation*).
- [ ] **Weighted / Deficit Round-Robin (DRR) Dispatcher**: Mengatur klaim job secara adil antar `tenant_id` atau `chain_id`.
- [ ] **Per-Tenant Concurrency Rate Limiter**: Membatasi eksekusi paralel maksimal untuk tenant tertentu agar tidak menghabiskan kuota worker pool.

### 1.3 Recurring & Cron Job Scheduler Engine
- [ ] **Cron Engine Integration**: Kemampuan mendefinisikan job berulang (*recurring jobs*) dengan sintaks Cron (misal: `0 0 * * *`) yang otomatis meng-enqueue job baru pada chain yang ditentukan secara presisi.
- [ ] **Scheduled / Delayed Jobs Management**: Peningkatan manajemen job dengan `available_at` di masa depan, dilengkapi index khusus agar tidak memperlambat klaim job instan.

### 1.4 Job DAG, Chaining & Dependency Workflow Engine
- [ ] **Cross-Chain / Multi-Job Dependencies (DAG)**: Mendukung kondisi di mana Job C pada Chain Z baru boleh berjalan jika Job A (Chain X) DAN Job B (Chain Y) telah selesai (`COMPLETED`).
- [ ] **Parent-Child Job Cascading**: Otomatis membatalkan atau menyelesaikan child jobs ketika parent job dibatalkan/gagal.

### 1.5 Progress Checkpointing & Stateful Execution
- [ ] **Long-Running Job Checkpoint API**: Memungkinkan handler menyimpan progress state parsial (misal: `ctx.SaveCheckpoint(payload)`). Jika worker *crash* di tengah jalan, retry akan melanjutkan dari checkpoint terakhir, bukan mengulang dari langkah 0.
- [ ] **Priority Queuing within Chain**: Job dengan prioritas lebih tinggi (*high priority flag*) dapat diproses lebih awal dalam rentang aturan urutan yang aman.

---

## 2. 🛡️ Resilience, Fault Tolerance & DLQ Engineering

Aspek ini memastikan pustaka tahan terhadap kegagalan infrastruktur, *unhandled panics*, dan memberikan kendali penuh terhadap penanganan error.

### 2.1 Worker Panic Recovery Guard
- [ ] **Panic Isolation & Stacktrace Capture**: Menangkap `panic` pada handler goroutine agar worker pool tidak pernah *crash*, mencatat *stack trace* lengkap ke log, dan menandai job sebagai `FAILED` terminally atau di-retry dengan aman.

### 2.2 Dynamic Circuit Breaker per Job Type / Service
- [ ] **Integration Circuit Breaker**: Jika layanan eksternal (seperti payment gateway atau email provider) *down*, circuit breaker otomatis memjeda (*pause*) klaim untuk `job_type` tersebut sementara waktu untuk mencegah *flapping* dan lonjakan retry gagal.

### 2.3 Advanced DLQ Operations & Management
- [ ] **Bulk Replay API & UI Action**: Kemampuan untuk melakukan *replay* masal pada ribuan job di DLQ berdasarkan filter (misal: rentang waktu, `job_type`, atau pesan error tertentu).
- [ ] **Bulk Skip & Bulk Purge**: Fitur pembersihan atau pengabaian masal untuk job yang sudah *obsolete*.
- [ ] **Payload Repair & Re-enqueue**: Fasilitas untuk mengubah *payload* JSON job yang salah format pada DLQ sebelum di-replay kembali ke antrean.
- [ ] **Automated Poison Pill Quarantine**: Otomatis mengidentifikasi job yang terus-menerus memicu *crash* dan mengisolasinya dari antrean utama.

---

## 3. 🔭 Enterprise Observability & OpenTelemetry

Observabilitas standar industri untuk memantau latensi, jejak terdistribusi, serta audit log lengkap.

### 3.1 Native OpenTelemetry (OTel) Integration
- [ ] **OTel Tracing Support**: Integrasi native dengan `go.opentelemetry.io/otel`. Otomatis meneruskan *TraceContext* (W3C traceparent) melalui `EnqueueRequest` hingga ke handler execution context.
- [ ] **OTel / Prometheus Metrics Exporter**: Menyediakan *standard metric counters & histograms*:
  - Queue latency distribution (p50, p95, p99)
  - Job execution duration per `job_type`
  - Active lease gauges & lease conflict counters
  - Chain blockage count & duration metrics

### 3.2 Audit Trail & Job Event State History
Saat ini tabel `ordered_jobs` hanya menyimpan status terakhir.
- [ ] **State History Table (`ordered_job_events`)**: Pencatatan riwayat setiap perubahan status job (misal: `PENDING` ➔ `PROCESSING` ➔ `RETRYING` ➔ `PROCESSING` ➔ `COMPLETED`) beserta timestamp, `worker_id`, durasi, dan `error_message` untuk analisa post-mortem.

### 3.3 Alerting Hooks & Event Webhooks
- [ ] **Engine Event Listeners**: Event callback interface untuk mendengarkan kejadian krusial:
  - `OnJobFailed(job, err)`
  - `OnChainBlocked(chainID, job)`
  - `OnDLQThresholdExceeded(count)`
- [ ] **Webhook Dispatcher**: Pengiriman notifikasi HTTP Webhook/Slack otomatis ketika antrean bermasalah.

---

## 4. 🧰 Developer Experience, Testing Utilities & Tooling

Memudahkan pengembang dalam membangun, menguji, dan memelihara aplikasi yang menggunakan `orderedjob`.

### 4.1 Testkit & In-Memory Mock Repository (`orderedjobtest`)
- [ ] **`orderedjobtest` Package**: Paket pembantu untuk *unit testing* aplikasi pengguna tanpa perlu menginstansiasi database PostgreSQL/MSSQL sungguhan.
- [ ] **Assertions Helper**: Helper seperti `assert.JobEnqueued(t, repo, chainID, jobType)` atau `assert.ChainCompleted(t, repo, chainID)`.

### 4.2 Middleware Pipeline Pattern for Handlers
- [ ] **Handler Middleware Support**: Mendukung *chaining* middleware pada handler, seperti:
  ```go
  engine.Use(LoggingMiddleware(), MetricsMiddleware(), RecoveryMiddleware())
  ```

### 4.3 Command Line Interface (`orderedjob-cli`)
- [ ] **CLI Binary Tool**: Perintah CLI standalone untuk operasi DevOps:
  - `orderedjob status` (menampilkan ringkasan antrean di terminal)
  - `orderedjob dlq replay --type=SEND_EMAIL --since=2h`
  - `orderedjob migrate up --db=postgres --url=...`

### 4.4 Strongly-Typed Handler Generators
- [ ] **Generic Payload Helper / CodeGen**: Peningkatan `RegisterTyped[T]` dengan schema validator (misal: JSON Schema / Struct validation) sebelum payload dieksekusi oleh handler.

---

## 5. 🖥️ Advanced Web UI & Management Dashboard

Meningkatkan kapabilitas antarmuka monitoring bawaan (`ui/`).

### 5.1 Real-Time Streaming (Server-Sent Events / WebSockets)
- [ ] **Live SSE/WebSocket Metrics**: Pembaruan grafik dan statistik secara *real-time* tanpa perlu perulangan *polling* HTTP biasa dari browser.

### 5.2 Interactive Payload Inspector & Modal Editor
- [ ] **Payload Viewer with Syntax Highlighting**: Tampilan JSON payload yang rapi dengan pencarian.
- [ ] **Edit & Replay Modal**: Form interaktif untuk memperbaiki isi payload JSON yang salah sebelum di-replay ke antrean.

### 5.3 Multi-tenant & Advanced Filtering UI
- [ ] **Tenant-aware Dashboard**: Filter antrean berdasarkan `tenant_id`, `trace_id`, dan rentang tanggal eksekusi.
- [ ] **Chain Dependency Graph Viewer**: Visualisasi status urutan job pada suatu chain secara grafis (diagram garis waktu urutan).

### 5.4 Web UI Security & Authentication
- [ ] **Auth Middleware & Protection**: Mendukung Basic Auth, JWT Token, atau custom HTTP middleware untuk mengamankan akses ke dashboard `/ui` dan REST API bawaannya.
- [ ] **RBAC (Role-Based Access Control)**: Membedakan hak akses `Read-Only Viewer` vs `Operator` (bisa Replay/Skip/Cancel job).

---

## 6. 🚀 High-Performance Storage, Compression & Housekeeping

Mencegah pembengkakan database (*database bloat*) dan mengoptimalkan penggunaan I/O pada skala jutaan job per hari.

### 6.1 Automated Housekeeping & Pruner Worker
- [ ] **Background Housekeeping Pruner**: Routine otomatis untuk membersihkan (*delete/archive*) job berstatus `COMPLETED` atau `CANCELLED` yang sudah melebihi batas retensi (misal: lebih tua dari 30 hari).

### 6.2 PostgreSQL Table Partitioning Strategy
- [ ] **Partitioning Migration Documentation & Scripts**: Skema tabel berbasis *Declarative Range Partitioning* (berdasarkan `created_at` atau `status`) untuk mengisolasi job aktif dari job historis agar ukuran indeks B-tree tetap kecil dan cepat.

### 6.3 Payload Compression & S3/Blob Offloading
- [ ] **Automatic GZIP Payload Compression**: Kompresi otomatis untuk payload berukuran sedang (> 10KB).
- [ ] **External Blob Storage Offloading**: Untuk payload besar (> 1MB), simpan payload di Object Storage (S3 / MinIO / GCS) dan hanya simpan referensi URI di database `ordered_jobs`.

---

## 7. 🧪 Reliability Engineering & Chaos Testing Suite

Memastikan pustaka teruji dalam kondisi paling ekstrim sebelum dirilis ke lingkungan enterprise.

### 7.1 Distributed Chaos Test Suite
- [ ] **Network Partition Simulation**: Pengujian perilaku *fencing token* (`lease_generation`) ketika worker mengalami penundaan jaringan (*split-brain/lagging*).
- [ ] **Database Connection Loss & Reconnect Test**: Pengujian resiliensi worker pool saat koneksi database terputus dan terhubung kembali secara tiba-tiba.

### 7.2 Scalability & Lock Contention Benchmarks
- [ ] **High-Concurrency Benchmarks**: Suite pengujian performa skala besar (100.000 concurrent chains) untuk mengukur throughput klaim dan mengdeteksi potensi *deadlock* pada Advisory Lock.

---

## 📌 Catatan Pelaksanaan (Execution Order Guidelines)

> [!TIP]
> **Rekomendasi Prioritas Pengembangan:**
> 1. **Fase 2.1 (Panic Recovery)** & **Fase 4.1 (Testkit)** - Pondasi keamanan dan kemudahan pengujian.
> 2. **Fase 3.1 (OpenTelemetry)** & **Fase 6.1 (Housekeeping Pruner)** - Kebutuhan operasional skala produksi.
> 3. **Fase 1.1 (Skip-on-Failure)** & **Fase 2.3 (Bulk DLQ Operations)** - Fitur kontrol antrean tingkat lanjut.
