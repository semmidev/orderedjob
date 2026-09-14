# OrderedJob Example Application

Aplikasi contoh yang menunjukkan penggunaan lengkap pustaka `orderedjob` dengan persistence database **PostgreSQL** dan **Microsoft SQL Server**, dilengkapi **Web UI Monitoring Dashboard**.

## Prasyarat

-  **Go 1.22+**
-  **Docker & Docker Compose** (atau instance PostgreSQL / SQL Server lokal)

## Cara Menjalankan

### 1. Jalankan Database via Docker Compose

```bash
docker compose up -d
```

Perintah di atas akan menjalankan:
-  **PostgreSQL**: Port `5432` (`orderedjob` DB, user `postgres`, password `postgres`)
-  **Microsoft SQL Server**: Port `1433` (user `sa`, password `StrongPassword123!`)

### 2. Pilih Database Engine via Flag `-db`

Gunakan flag `-db` untuk memilih engine database saat menjalankan program:

```bash
# Menggunakan SQL Server (default)
go run main.go -db sqlserver

# Menggunakan PostgreSQL
go run main.go -db postgres
```

> **Default**: jika flag `-db` tidak disebutkan, program akan menggunakan `sqlserver`.

### 3. Kustomisasi DSN via Environment Variable

Jika menggunakan instance database yang berbeda, atur variabel lingkungan sebelum menjalankan:

```bash
# PostgreSQL
DATABASE_URL="postgres://username:password@localhost:5432/my_db?sslmode=disable" \
go run main.go -db postgres

# SQL Server
MSSQL_URL="sqlserver://sa:MyPassword!@localhost:1433?database=master&encrypt=disable" \
go run main.go -db sqlserver
```

### 4. Buka Web UI Dashboard

Setelah program berjalan, buka browser ke:

```
http://localhost:8080/ui
```

Dashboard menyediakan:
-  Monitoring job secara real-time
-  Filter berdasarkan status, job type, dan chain
-  **Enqueue New Job** dengan dropdown Job Type yang otomatis terisi dari handler yang telah didaftarkan

### 5. Hentikan Container Database

```bash
docker compose down -v
```

---

## Struktur Program (`main.go`)

```
main()
├── flag -db         → pilih postgres atau sqlserver
├── connectPostgres()   → koneksi, ping, migrate (PG)
├── connectSQLServer()  → koneksi, ping, migrate (MSSQL)
├── startServer()       → build engine + HTTP server (generik)
├── buildEngine()       → register semua handlers
└── runEngineDemoLongLived()  → start engine + seed jobs + block
```

---

## Fitur-Fitur Utama yang Ditunjukkan (`main.go`)

### 1. Multi-Database Persistence & Otomatisasi Skema
-  **PostgreSQL Repository (`pgRepo.New`)**: Menggunakan connection pool `pgxpool.Pool`.
-  **SQL Server Repository (`mssqlRepo.New`)**: Menggunakan `database/sql` dan T-SQL `UPDLOCK, READPAST`.
-  **Auto Migration**: Menjalankan migrasi DDL tabel `ordered_jobs` secara otomatis via `repo.Migrate(ctx)`.

### 2. Konfigurasi Engine & Worker Pool
-  **`WithConcurrency(5)`**: Menjalankan 5 worker goroutine secara paralel.
-  **`WithPollInterval(100ms)`**: Interval polling job yang responsif.
-  **`WithLease(15s)`**: Batas waktu kepemilikan lock job sebelum dianggap stale.
-  **`WithSlogLogger(logger)`**: Integrasi terstruktur dengan `log/slog` bawaan Go.
-  **`WithNotify(true)`**: Bangunkan worker secara cepat saat job baru datang.
-  **`WithRetryPolicy(...)`**: Strategi retry berbasis exponential backoff dengan jitter.

### 3. Pendaftaran Handler (Type-Safe Generics & Dynamic)
-  **`RegisterTyped[T]`**: Handler dengan deserialisasi payload JSON otomatis via Go Generics.
-  **`RegisterFunc`**: Handler langsung menggunakan `json.RawMessage` (contoh: `AuditLog`).
-  **`Engine.JobTypes()`**: Mengekspos daftar job type yang terdaftar agar UI dapat menampilkan dropdown.

### 4. Web UI & API Endpoints
-  **`/api/job-types`**: Mengembalikan daftar job type yang terdaftar di engine (digunakan oleh dropdown modal Enqueue).
-  **`ui.WithJobTypesProvider(eng)`**: Menghubungkan engine ke UI agar job types tersedia sejak server pertama kali naik.

### 5. Distributed Tracing (Trace Context Propagation)
-  **`WithTraceID(ctx, traceID)`**: Menyisipkan trace ID ke dalam context saat enqueue.
-  **`ExtractTraceID(ctx)`**: Membaca trace ID dari context di dalam handler untuk korelasi log end-to-end.

### 6. Penanganan Error Transien vs Permanen
-  **`orderedjob.Retryable(err)`**: Menandai error transien agar worker melakukan retry.
-  **Error non-retryable**: Job langsung dipindahkan ke status terminal tanpa retry.

### 7. Jaminan Urutan FIFO & Enqueue
-  **Explicit Sequence FIFO**: Menjamin urutan eksekusi tepat sesuai `Sequence = 1, 2, 3...` per chain.
-  **Auto-Sequence (`Sequence = 0`)**: Menggenerasi indeks urutan berikutnya secara otomatis di sisi database.
-  **Idempotency Key**: Mencegah duplikasi enqueue akibat percobaan ulang client.
-  **Multi-Tenant (`TenantID`)**: Mengisolasi eksekusi antar tenant.

### 8. Graceful Shutdown
-  **`eng.Shutdown(ctx)`**: Menghentikan pengambilan job baru dan menunggu pekerjaan yang sedang berjalan selesai.

---

## Standar Metrik (Prometheus / OpenTelemetry)

Pustaka `orderedjob` menyediakan antarmuka `orderedjob.Metrics` yang sesuai dengan konvensi **Prometheus** dan **OpenTelemetry Messaging Specs**.

| Method Interface | Jenis Metrik | Nama Metrik Standar | Deskripsi |
| :--- | :--- | :--- | :--- |
| `IncClaimed()` | Counter | `orderedjob_jobs_claimed_total` | Total job yang berhasil diklaim worker |
| `IncCompleted(jobType)` | Counter | `orderedjob_jobs_completed_total` | Total job yang sukses diselesaikan |
| `IncFailed(jobType)` | Counter | `orderedjob_jobs_failed_total` | Total job yang gagal permanen |
| `IncRetry(jobType)` | Counter | `orderedjob_job_retries_total` | Total percobaan ulang job |
| `ObserveExecDuration(jobType, d)` | Histogram | `orderedjob_job_execution_duration_seconds` | Durasi eksekusi handler (detik) |
| `ObserveQueueDelay(jobType, d)` | Histogram | `orderedjob_job_queue_delay_seconds` | Waktu tunggu job dari `available_at` hingga dieksekusi |
| `SetActiveLeases(n)` | Gauge | `orderedjob_active_leases` | Jumlah lease aktif saat ini |
| `IncClaimConflicts()` | Counter | `orderedjob_claim_conflicts_total` | Total benturan klaim lock |
| `IncStaleRecovered(n)` | Counter | `orderedjob_stale_recovered_total` | Total job stale yang dipulihkan |
| `IncBlockedChain()` | Counter | `orderedjob_blocked_chains_total` | Total chain yang terhenti karena urutan belum selesai |
