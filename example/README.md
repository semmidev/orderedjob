# OrderedJob Example Application

Aplikasi contoh yang menunjukkan penggunaan lengkap pustaka `orderedjob` dengan persistence database **PostgreSQL** dan **Microsoft SQL Server**.

## Prasyarat

- **Go 1.22+**
- **Docker & Docker Compose** (atau instance PostgreSQL & SQL Server lokal)

## Cara Menjalankan

### 1. Jalankan PostgreSQL & SQL Server via Docker Compose

Gunakan Docker Compose untuk menjalankan container PostgreSQL dan SQL Server secara bersamaan:

```bash
docker compose up -d
```

Perintah di atas akan menjalankan:
- **PostgreSQL**: Port `5432` (`orderedjob` DB, user `postgres`, password `postgres`).
- **Microsoft SQL Server**: Port `1433` (user `sa`, password `StrongPassword123!`).

### 2. Jalankan Program Contoh

Jalankan file `main.go`:

```bash
go run main.go
```

Program `main.go` akan mengeksekusi dua sesi demo secara berurutan:
1. **Bagian 1**: Demo alur kerja dengan **PostgreSQL** repository adapter.
2. **Bagian 2**: Demo alur kerja dengan **Microsoft SQL Server** repository adapter.

Jika kamu menggunakan instance database yang berbeda, kamu dapat mengatur variabel lingkungan:

```bash
DATABASE_URL="postgres://username:password@localhost:5432/my_db?sslmode=disable" \
MSSQL_URL="sqlserver://sa:StrongPassword123!@localhost:1433?database=master&encrypt=disable" \
go run main.go
```

### 3. Hentikan Container Database

Untuk menghentikan dan membersihkan container serta volume database:

```bash
docker compose down -v
```

---

## Fitur-Fitur Utama yang Ditunjukkan (`main.go`)

Program contoh ini mencakup demonstrasi dari seluruh kemampuan pustaka `orderedjob` pada kedua engine database:

### 1. Multi-Database Persistence & Otomatisasi Skema
- **PostgreSQL Repository (`pgRepo.New`)**: Menggunakan connection pool `pgxpool.Pool` dan perintah DDL PostgreSQL.
- **SQL Server Repository (`mssqlRepo.New`)**: Menggunakan `database/sql` (`*sql.DB`) dan T-SQL `UPDLOCK, READPAST`.
- **Auto Migration**: Menjalankan migrasi DDL tabel `ordered_jobs` secara otomatis via `repo.Migrate(ctx)`.

### 2. Konfigurasi Engine & Worker Pool
- **`WithConcurrency(5)`**: Menjalankan 5 worker goroutine secara paralel.
- **`WithPollInterval(100ms)`**: Interval polling job yang responsif.
- **`WithLease(15s)`**: Batas waktu kepemilikan lock job sebelum dianggap lelap (*stale*).
- **`WithSlogLogger(logger)`**: Integrasi terstruktur dengan `log/slog` bawaan Go.
- **`WithNotify(true)`**: Bangunkan worker secara cepat saat job baru datang.
- **`WithRetryPolicy(...)`**: Strategi percobaan ulang (*retry*) berbasis exponential backoff dengan jitter.

### 3. Pendaftaran Handler (Type-Safe Generics & Dynamic)
- **Type-Safe Handlers (`RegisterTyped[T]`)**: Pendaftaran handler dengan deserialisasi payload JSON otomatis berbasis Go Generics untuk `OrderPayload` dan `NotificationPayload`.
- **Dynamic Raw Handler (`RegisterFunc`)**: Pendaftaran handler langsung menggunakan payload `json.RawMessage` (contoh: `AuditLog`).

### 4. Distributed Tracing (Trace Context Propagation)
- **`WithTraceID(ctx, traceID)`**: Menyisipkan trace ID ke dalam context saat enqueue.
- **`ExtractTraceID(ctx)`**: Membaca trace ID dari context di dalam handler untuk korelasi log end-to-end.

### 5. Penanganan Error Transien vs Permanen
- **`orderedjob.Retryable(err)`**: Menandai error transien (misal: timeout jaringan) agar worker melakukan retry sesuai kebijakan backoff.
- **Error Non-Retryable**: Jika error biasa dikembalikan, job langsung dipindahkan ke status terminal/DLQ tanpa retry tak terbatas.

### 6. Mode Enkui & Jaminan Urutan FIFO
- **Explicit Sequence FIFO**: Menjamin urutan eksekusi pekerjaan tepat sesuai indeks urutan (`Sequence = 1, 2, 3...`) pada tiap chain isolasi.
- **Single-Transaction Batch Enqueue (`EnqueueBatch`)**: Memasukkan banyak job sekaligus dalam 1 transaksi database yang atomic.
- **Auto-Sequence (`Sequence = 0`)**: Menggenerasi indeks urutan berikutnya secara otomatis di sisi database (`MAX(sequence) + 1`).

### 7. Pengisolasian Tenant, Idempotensi, & Penjadwalan
- **Idempotency Key**: Mencegah duplikasi enqueue job akibat percobaan ulang HTTP/gRPC client.
- **Multi-Tenant (`TenantID`)**: Mengisolasi eksekusi pekerjaan antar tenant aplikasi.
- **Scheduled Delay (`AvailableAt`)**: Menunda eksekusi job hingga waktu yang ditentukan di masa depan.

### 8. Graceful Shutdown
- **`eng.Shutdown(ctx)`**: Menghentikan pengambilan job baru dan menunggu seluruh pekerjaan yang sedang berjalan selesai secara bersih.

---

## Standar Metrik (Prometheus / OpenTelemetry Best Practices)

Pustaka `orderedjob` menyediakan antarmuka `orderedjob.Metrics` yang dirancang agar sesuai dengan konvensi penamaan standar **Prometheus** dan **OpenTelemetry Messaging Specs**.

### Pemetaan Antarmuka Metrik

| Method Interface | Jenis Metrik Prometheus | Nama Metrik Standar | Deskripsi |
| :--- | :--- | :--- | :--- |
| `IncClaimed()` | Counter | `orderedjob_jobs_claimed_total` | Total job yang berhasil diklaim oleh worker |
| `IncCompleted(jobType)` | Counter | `orderedjob_jobs_completed_total` | Total job yang sukses diselesaikan (berdasarkan `job_type`) |
| `IncFailed(jobType)` | Counter | `orderedjob_jobs_failed_total` | Total job yang gagal secara permanen |
| `IncRetry(jobType)` | Counter | `orderedjob_job_retries_total` | Total percobaan ulang job |
| `ObserveExecDuration(jobType, d)` | Histogram | `orderedjob_job_execution_duration_seconds` | Durasi eksekusi handler job (detik) |
| `ObserveQueueDelay(jobType, d)` | Histogram | `orderedjob_job_queue_delay_seconds` | Waktu tunggu job dari `available_at` hingga mulai dieksekusi |
| `SetActiveLeases(n)` | Gauge | `orderedjob_active_leases` | Jumlah lease job yang aktif saat ini |
| `IncClaimConflicts()` | Counter | `orderedjob_claim_conflicts_total` | Total benturan klaim lock (*optimistic lock conflict*) |
| `IncStaleRecovered(n)` | Counter | `orderedjob_stale_recovered_total` | Total job lelap (*stale lease*) yang dipulihkan kembali |
| `IncBlockedChain()` | Counter | `orderedjob_blocked_chains_total` | Total chain yang terhenti akibat urutan pekerjaan yang belum selesai |
