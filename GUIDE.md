# Panduan Lengkap & Cetak Biru Fitur (`GUIDE.md`)

Selamat datang di panduan resmi penggunaan **`orderedjob`**. Dokumen ini dirancang sebagai referensi komprehensif yang menjelaskan **setiap opsi konfigurasi, parameter API, fungsi, efek samping (*side effects*)**, serta skenario penggunaan dari tingkat paling dasar (*basic*) hingga tingkat mahir (*enterprise advanced*).

---

## Daftar Isi

-  [Kamus Referensi Opsi Engine & API Parameter](#kamus-referensi-opsi-engine--api-parameter)
-  [1. Skenario 1: Dasar & In-Memory Quickstart](#1-skenario-1-dasar--in-memory-quickstart)
-  [2. Skenario 2: Auto-Sequence & Multi-Step Ordering](#2-skenario-2-auto-sequence--multi-step-ordering)
-  [3. Skenario 3: Multi-Database Production Engine (PostgreSQL & SQL Server)](#3-skenario-3-multi-database-production-engine-postgresql--sql-server)
-  [4. Skenario 4: Policy-Driven Ordering Modes & Dynamic Strategy Resolver](#4-skenario-4-policy-driven-ordering-modes--dynamic-strategy-resolver)
-  [5. Skenario 5: Penanganan Error, Kebijakan Retry & Exponential Backoff Jitter](#5-skenario-5-penanganan-error-kebijakan-retry--exponential-backoff-jitter)
-  [6. Skenario 6: Worker Panic Recovery Isolation & Interceptor](#6-skenario-6-worker-panic-recovery-isolation--interceptor)
-  [7. Skenario 7: Type-Safe Handlers & Struct Schema Validation](#7-skenario-7-type-safe-handlers--struct-schema-validation)
-  [8. Skenario 8: Scheduled & Delayed Job Management](#8-skenario-8-scheduled--delayed-job-management)
-  [9. Skenario 9: Enterprise Observability (OpenTelemetry Tracing & Metrics)](#9-skenario-9-enterprise-observability-opentelemetry-tracing--metrics)
-  [10. Skenario 10: Event Callback Listeners & HTTP Webhook Dispatcher](#10-skenario-10-event-callback-listeners--http-webhook-dispatcher)
-  [11. Skenario 11: Manajemen DLQ Lanjutan & Bulk Operations API](#11-skenario-11-manajemen-dlq-lanjutan--bulk-operations-api)
-  [12. Skenario 12: Web UI Dashboard & Real-Time SSE Streaming](#12-skenario-12-web-ui-dashboard--real-time-sse-streaming)
-  [13. Skenario 13: End-to-End Enterprise Master Pipeline (Semua Fitur Digabung)](#13-skenario-13-end-to-end-enterprise-master-pipeline-semua-fitur-digabung)

---

## Kamus Referensi Opsi Engine & API Parameter

### Matriks Opsi Engine (`orderedjob.Option`)

Tabel berikut menjelaskan seluruh 19 parameter konfigurasi `orderedjob.Option` yang tersedia saat menginisialisasi engine via `orderedjob.New(repo, opts...)`:

| Opsi Konfigurasi | Tipe Data | Nilai Default | Deskripsi & Fungsi Utama | Efek Samping (*Side Effects*) & Pertimbangan Performa |
| :--- | :--- | :--- | :--- | :--- |
| **`WithConcurrency(n)`** | `int` | `10` | Menentukan jumlah worker goroutine yang berjalan secara simultan memproses pekerjaan. | Nilai lebih tinggi meningkatkan throughput eksekusi antar-chain, namun memakai lebih banyak memory & koneksi database pool. |
| **`WithPollInterval(d)`** | `time.Duration` | `500ms` | Interval perulangan polling periodik worker scanner saat mencari job yang siap diklaim. | Nilai lebih kecil (misal `10ms`) mempercepat respon klaim fallback, namun meningkatkan jumlah query `SELECT` saat antrean kosong jika `WithNotify` tidak aktif. |
| **`WithLease(d)`** | `time.Duration` | `60s` | Durasi sewa (*lease duration*) eksklusif yang diberikan kepada worker saat mengklaim suatu job. | Heartbeat goroutine internal memperpanjang lease ini setiap `d / 3`. Jika worker mati mendadak, job baru bisa di-reclaim setelah durasi `d` kadaluarsa. |
| **`WithRecoveryInterval(d)`** | `time.Duration` | `10s` | Interval pengujian rutin worker pemulihan (*Stale Lease Recovery*) untuk memindai job menggantung akibat worker crash. | Memindai job `PROCESSING` dengan `lease_until < NOW()`. Nilai lebih kecil mempercepat pemulihan job pasca crash. |
| **`WithWorkerID(id)`** | `string` | `worker-<uuid>` | Identifikasi unik instance worker engine ini dalam kluster terdistribusi. | Dicatat pada kolom `worker_id` di database untuk audit jejak eksekusi dan verifikasi *fencing token* (`lease_generation`). |
| **`WithNotify(enabled)`** | `bool` | `false` | Mengaktifkan listener terstruktur sinyal PostgreSQL `LISTEN/NOTIFY`. | Mengabaikan keterlambatan polling periodik saat job baru di-enqueue, menghasilkan latensi klaim instan `< 5ms`. Membutuhkan 1 koneksi DB pgx dedicated. |
| **`WithRetryPolicy(p)`** | `retry.Policy` | `Max 5, Base 1s, Max 5m` | Kebijakan percobaan ulang (*exponential backoff + jitter*) untuk eror transien (`orderedjob.Retryable`). | Job yang gagal sementara akan di-retry hingga `MaxAttempts` sebelum dipindahkan ke status `FAILED` / DLQ. |
| **`WithOrderingMode(mode)`** | `string` | `"strict"` | Mode urutan global engine (`"strict"`, `"skip-on-failure"`, `"dead-letter-and-continue"`). | Menentukan apakah kegagalan job memblokir urutan berikutnya (`Sequence N+1`) atau melewatinya secara otomatis. |
| **`WithOrderingStrategy(s)`** | `OrderingStrategyResolver` | `nil` | Resolver fungsi dinamis untuk memilih mode urutan per request `EnqueueRequest`. | Meng-override `WithOrderingMode` global secara dinamis berdasarkan `job_type`, `tenant_id`, atau atribut request. |
| **`WithPanicHandler(h)`** | `PanicHandler` | `nil` | Interceptor kustom saat handler goroutine mengalami *unhandled runtime panic*. | Menangkap panic value dan stack trace mentah tanpa mematikan worker pool atau aplikasi utama. |
| **`WithRetryOnPanic(retry)`** | `bool` | `false` | Menentukan apakah panic pada handler wajib di-retry sesuai Retry Policy. | Jika `true`, panic dianggap eror transien dan di-retry. Jika `false` (default), panic langsung mengubah status job ke `FAILED`. |
| **`WithMetrics(m)`** | `Metrics` | `NoopMetrics` | Collector metrik OpenTelemetry / Prometheus (`NewOTelMetrics`). | Mencatat durasi eksekusi, queue delay, counter job completed/failed/panics/stale. Overhead atomic memori sangat kecil. |
| **`WithLogger(l)`** | `Logger` | `NoopLogger` | Interface logger internal pustaka `orderedjob`. | Mencatat aktivitas klaim, recovery, dan error internal engine. |
| **`WithSlogLogger(l)`** | `*slog.Logger` | `nil` | Adapter integrasi terstruktur `log/slog` standar Go 1.21+. | Menghasilkan log berformat JSON/Text terstruktur dengan level log terpadu. |
| **`WithTracer(t)`** | `trace.Tracer` | OTel Global | Tracer OpenTelemetry untuk distributed tracing terdistribusi. | Meneruskan `traceparent` W3C TraceContext dari enkui hingga ke handler context. |
| **`WithTracerProvider(tp)`** | `trace.TracerProvider` | `nil` | Provider tracer OpenTelemetry kustom. | Menginstansiasi tracer khusus `github.com/semmidev/orderedjob`. |
| **`WithEventListener(l)`** | `EventListener` | `NoopEventListener` | Callback interface untuk event lifecycle antrean (`OnJobClaimed`, `OnJobCompleted`, `OnJobFailed`, `OnChainBlocked`, `OnDLQThresholdExceeded`). | Dipanggil saat event terjadi. Pastikan logika di dalam callback tidak memblokir worker execution thread. |
| **`WithWebhook(cfg)`** | `WebhookConfig` | `nil` | Dispatcher notifikasi HTTP Webhook otomatis. | Mengirimkan HTTP POST JSON/Slack payload saat event `OnJobFailed` atau `OnChainBlocked` dipicu. |
| **`WithDLQThreshold(n)`** | `int64` | `0` (Disabled) | Batas ambang jumlah job gagal di DLQ untuk memicu callback `OnDLQThresholdExceeded`. | Mengirimkan sinyal peringatan jika akumulasi job gagal di DLQ telah melebihi `n`. |

---

### Parameter `orderedjob.EnqueueRequest`

| Parameter Field | Tipe Data | Wajib/Opsional | Deskripsi & Fungsi |
| :--- | :--- | :--- | :--- |
| `ChainID` | `string` | **Wajib** | Identifier unik rantai antrean (misal: ID akun, ID pesanan, ID tenant). Job dalam ChainID yang sama dieksekusi secara FIFO sekuensial. |
| `Sequence` | `int64` | **Wajib** | Nomor urutan eksekusi (`>= 1` untuk urutan eksplisit, `0` untuk alokasi otomatis *Auto-Sequence*). |
| `Type` | `string` | **Wajib** | Nama tipe job yang terdaftar pada handler registry engine via `RegisterTyped` / `RegisterFunc`. |
| `Payload` | `any` | **Wajib** | Struct atau map data yang akan diserialisasi ke JSON payload. |
| `IdempotencyKey` | `string` | Opsional | Key unik untuk mencegah duplikasi enkui akibat retry client HTTP. |
| `TenantID` | `string` | Opsional | Identifier isolasi multi-tenant untuk filtering dan rate-limiting. |
| `TraceID` | `string` | Opsional | Identifier penelusuran terdistribusi (*Distributed Tracing*). |
| `OrderingMode` | `string` | Opsional | Override mode urutan khusus job ini (`strict`, `skip-on-failure`, `dead-letter-and-continue`). |
| `MaxAttempts` | `int` | Opsional | Override batas maksimal percobaan ulang retry khusus job ini. |
| `DeadlineAt` | `*time.Time` | Opsional | Batas waktu maksimal eksekusi job sebelum dianggap kadaluarsa. |
| `AvailableAt` | `*time.Time` | Opsional | Timestamp kapan job mulai diizinkan diklaim oleh worker scanner. |

---

### Parameter Filter `orderedjob.JobFilter` (DLQ & Searching)

| Filter Field | Tipe Data | Deskripsi & Fungsi |
| :--- | :--- | :--- |
| `ChainID` | `string` | Memfilter pencarian berdasarkan ID rantai tertentu. |
| `JobType` | `string` | Memfilter berdasarkan nama tipe job. |
| `TenantID` | `string` | Memfilter berdasarkan ID tenant. |
| `Status` | `string` | Memfilter berdasarkan status (`PENDING`, `PROCESSING`, `RETRYING`, `FAILED`, `CANCELLED`, `DEAD_LETTERED`, `COMPLETED`). |
| `TraceID` | `string` | Memfilter berdasarkan Trace ID penelusuran. |
| `ErrorMessage` | `string` | Memfilter job yang mengandung potongan teks error tertentu pada kolom `last_error`. |
| `CreatedBefore` | `time.Time` | Memfilter job yang dibuat sebelum timestamp ini. |
| `CreatedAfter` | `time.Time` | Memfilter job yang dibuat setelah timestamp ini. |
| `Limit` | `int` | Batas jumlah baris data yang dikembalikan (pagination). |
| `Offset` | `int` | Indeks pergeseran baris data (pagination). |

---

## 1. Skenario 1: Dasar & In-Memory Quickstart

### Fungsi Utama
Menjalankan `orderedjob` menggunakan adapter memori (`repository/memory`) tanpa dependensi database eksternal. Cocok untuk unit testing, pengujian lokal, atau pemrosesan antrean internal aplikasi.

### Konfigurasi & Opsi Terlibat
-  `memory.New()`: Inisialisasi storage in-memory berbasis Go struct & muteks.
-  `WithConcurrency(5)`: Mengonfigurasi 5 worker goroutines.
-  `WithPollInterval(100*time.Millisecond)`: Mengatur interval polling scanner ke 100ms.
-  `RegisterTyped[T]`: Mendaftarkan fungsi handler *type-safe* dengan unmarshaling JSON otomatis.

### Efek Samping (*Side Effects*)
-  Data antrean disimpan di RAM volatil. Restart aplikasi akan menghapus seluruh data antrean.
-  Penguncian rantai dilakukan di memori lokal (tidak terdistribusi antar node).

### Contoh Kode Go

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

type NotificationPayload struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

func main() {
	ctx := context.Background()

	// 1. Inisialisasi In-Memory Repository
	repo := memory.New()

	// 2. Inisialisasi Engine dengan Concurrency & Poll Interval
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(5),
		orderedjob.WithPollInterval(100*time.Millisecond),
		orderedjob.WithWorkerID("local-worker-1"),
	)

	// 3. Daftarkan Handler Type-Safe
	orderedjob.RegisterTyped(eng, "SendNotification", func(ctx context.Context, job orderedjob.Job, payload NotificationPayload) error {
		fmt.Printf("[WORKER %s] Mengirim notifikasi ke User %s: %s (Seq: %d)\n", job.WorkerID, payload.UserID, payload.Message, job.Sequence)
		return nil
	})

	// 4. Jalankan Engine
	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Gagal menjalankan engine: %v", err)
	}
	defer eng.Shutdown(ctx)

	// 5. Enqueue Job Berurutan (Sequence 1)
	_, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
		ChainID:  "user-101",
		Sequence: 1,
		Type:     "SendNotification",
		Payload:  NotificationPayload{UserID: "user-101", Message: "Selamat Datang!"},
	})
	if err != nil {
		log.Fatalf("Enqueue gagal: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
}
```

---

## 2. Skenario 2: Auto-Sequence & Multi-Step Ordering

### Fungsi Utama
Pengalokasian penomoran urutan otomatis (`Sequence = 0`) tanpa perlu menghitung manual sequence dari sisi aplikasi pengirim. Engine mengeksekusi *Advisory Lock* aman untuk menjamin urutan bertahap (*Step 1  Step 2  Step 3*).

### Konfigurasi & Opsi Terlibat
-  `Sequence: 0`: Memicu mekanisme *Auto-Sequence Allocation*.
-  **PostgreSQL**: `SELECT pg_advisory_xact_lock(hashtext(chain_id))`.
-  **SQL Server**: `EXEC sp_getapplock @Resource = chainID`.

### Efek Samping (*Side Effects*)
-  Mengambil penguncian eksklusif singkat pada database per `chain_id` saat enkui untuk mencegah *race condition* nomor urutan.
-  Job berikutnya (`Sequence N+1`) akan berstatus **`BLOCKED`** sampai job sebelumnya (`Sequence N`) berstatus **`COMPLETED`**.

### Contoh Kode Go

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

type StepPayload struct {
	StepName string `json:"step_name"`
}

func main() {
	ctx := context.Background()
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithConcurrency(2))

	orderedjob.RegisterTyped(eng, "OrderPipeline", func(ctx context.Context, job orderedjob.Job, payload StepPayload) error {
		fmt.Printf("==> [Chain: %s] [Seq: %d] Executing Step: %s\n", job.ChainID, job.Sequence, payload.StepName)
		return nil
	})

	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	chainID := "order-trx-999"

	// Enqueue 3 Tahapan Workflow dengan Sequence = 0 (Auto-Sequence)
	steps := []string{"1. Validate Cart", "2. Reserve Inventory", "3. Charge Credit Card"}
	for _, step := range steps {
		job, err := eng.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  chainID,
			Sequence: 0, // Auto-sequence!
			Type:     "OrderPipeline",
			Payload:  StepPayload{StepName: step},
		})
		if err != nil {
			log.Fatalf("Enqueue step gagal: %v", err)
		}
		fmt.Printf("[ENQUEUE] Success JobID: %s -> Assigned Sequence: %d\n", job.ID, job.Sequence)
	}

	time.Sleep(1 * time.Second)
}
```

---

## 3. Skenario 3: Multi-Database Production Engine (PostgreSQL & SQL Server)

### Fungsi Utama
Menhubungkan `orderedjob` ke database relational skala produksi (**PostgreSQL** atau **Microsoft SQL Server**) dengan isolasi *fencing token* (`lease_generation`) dan pemindahan state terdistribusi.

### Konfigurasi & Opsi Terlibat
-  `postgres.New(pool)`: Adapter PostgreSQL berbasis driver `pgxpool.Pool`.
-  `sqlserver.New(db)`: Adapter SQL Server berbasis driver standard `*sql.DB` (`go-mssqldb`).
-  `repo.Migrate(ctx)`: Mengeksekusi DDL migrasi otomatis pemuatan tabel `ordered_jobs` dan indeks parsial.
-  `WithNotify(true)`: Mendengarkan event PostgreSQL `LISTEN/NOTIFY` untuk latensi klaim `< 5ms`.
-  `WithLease(30*time.Second)`: Durasi sewa eksklusif per worker.
-  `WithRecoveryInterval(5*time.Second)`: Pemindaian job tertinggal dari worker mati.

### Efek Samping (*Side Effects*)
-  Kueri klaim menggunakan `FOR UPDATE SKIP LOCKED` (PostgreSQL) atau `WITH (UPDLOCK, READPAST)` (SQL Server) yang mengisolasi baris job aktif tanpa memblokir koneksi DB lainnya.

### Contoh Kode Go

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/semmidev/orderedjob"
	pgRepo "github.com/semmidev/orderedjob/repository/postgres"
	msRepo "github.com/semmidev/orderedjob/repository/sqlserver"
)

func runPostgresEngine(ctx context.Context, connString string) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		log.Fatalf("Gagal connect PG: %v", err)
	}

	repo := pgRepo.New(pool)
	if err := repo.Migrate(ctx); err != nil {
		log.Fatalf("Migrasi skema PG gagal: %v", err)
	}

	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(10),
		orderedjob.WithNotify(true), // LISTEN/NOTIFY instan
		orderedjob.WithLease(30*time.Second),
		orderedjob.WithRecoveryInterval(5*time.Second),
	)
	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)
}

func runSQLServerEngine(ctx context.Context, connString string) {
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatalf("Gagal connect MSSQL: %v", err)
	}

	repo := msRepo.New(db)
	if err := repo.Migrate(ctx); err != nil {
		log.Fatalf("Migrasi skema MSSQL gagal: %v", err)
	}

	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(10),
		orderedjob.WithPollInterval(200*time.Millisecond),
		orderedjob.WithLease(30*time.Second),
	)
	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)
}
```

---

## 4. Skenario 4: Policy-Driven Ordering Modes & Dynamic Strategy Resolver

### Fungsi Utama
Menyesuaikan perilaku penanganan urutan rantai (*chain ordering*) saat terjadi kegagalan job.

### Pilihan Mode Urutan
1. **`OrderingModeStrict` (`"strict"`)**: (Default) Jika Job `N-1` gagal, chain **terhenti** (`BLOCKED`) untuk keamanan data sekuensial.
2. **`OrderingModeSkipOnFailure` (`"skip-on-failure"`)**: Jika Job `N-1` gagal terminal, engine otomatis melewatinya dan membuka Job `N` ke `PENDING`.
3. **`OrderingModeDeadLetterAndContinue` (`"dead-letter-and-continue"`)**: Mengisolasi Job `N-1` ke DLQ tanpa menahan urutan Job `N`.

### Dynamic Strategy Resolver
-  `WithOrderingStrategy(func(req EnqueueRequest) string)`: Menentukan mode urutan secara dinamis per `job_type`, `tenant_id`, atau atribut request.

### Efek Samping (*Side Effects*)
-  Mode `skip-on-failure` atau `dead-letter-and-continue` mengizinkan eksekusi job berikutnya meskipun langkah sebelumnya gagal. Pastikan logika aplikasi Anda tahan terhadap langkah parsial yang terlewati.

### Contoh Kode Go

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

func main() {
	ctx := context.Background()
	repo := memory.New()

	// Engine dengan Strategi Dinamis per Job Type
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(2),
		orderedjob.WithOrderingMode(orderedjob.OrderingModeStrict), // Default global
		orderedjob.WithOrderingStrategy(func(req orderedjob.EnqueueRequest) string {
			if req.Type == "TelemetryLog" {
				return orderedjob.OrderingModeSkipOnFailure // Jangan pernah blokir antrean telemetry
			}
			return orderedjob.OrderingModeStrict // Strict untuk transaksi finansial
		}),
	)

	orderedjob.RegisterTyped(eng, "TelemetryLog", func(ctx context.Context, job orderedjob.Job, payload map[string]string) error {
		if job.Sequence == 1 {
			return orderedjob.NonRetryable(errors.New("kegagalan sensor 1"))
		}
		fmt.Printf("Telemetry Step %d Berhasil!\n", job.Sequence)
		return nil
	})

	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	chainID := "sensor-node-01"
	_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: chainID, Sequence: 1, Type: "TelemetryLog"})
	_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: chainID, Sequence: 2, Type: "TelemetryLog"})

	time.Sleep(500 * time.Millisecond)
}
```

---

## 5. Skenario 5: Penanganan Error, Kebijakan Retry & Exponential Backoff Jitter

### Fungsi Utama
Mengendalikan penanganan eror transien (seperti penundaan jaringan/timeout API) dengan *exponential backoff + random jitter* agar tidak membebankan layanan eksternal.

### Konfigurasi & Opsi Terlibat
-  `orderedjob.Retryable(err)`: Menandai eror sebagai kesalahan transien yang wajib di-retry.
-  `orderedjob.NonRetryable(err)`: Menandai eror sebagai kesalahan terminal yang langsung menghentikan job ke `FAILED`.
-  `WithRetryPolicy(retry.Policy{...})`:
  -  `MaxAttempts`: Batas percobaan ulang.
  -  `BaseDelay`: Penundaan awal.
  -  `MaxDelay`: Batas maksimum penundaan.
  -  `Jitter`: Option `retry.NoJitter`, `retry.FullJitter`, atau `retry.EqualJitter`.

### Efek Samping (*Side Effects*)
-  Job yang di-retry akan berstatus `RETRYING` dan `available_at` diperbarui ke masa depan. Selama masa backoff, urutan berikutnya (`Sequence N+1`) tetap aman di status `BLOCKED`.

### Contoh Kode Go

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
	"github.com/semmidev/orderedjob/retry"
)

func main() {
	ctx := context.Background()
	repo := memory.New()

	// Kustomisasi Retry Policy
	customRetry := retry.Policy{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    1 * time.Second,
		Jitter:      retry.FullJitter,
	}

	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(2),
		orderedjob.WithRetryPolicy(customRetry),
	)

	var attemptCount int

	orderedjob.RegisterTyped(eng, "HTTPCall", func(ctx context.Context, job orderedjob.Job, payload map[string]string) error {
		attemptCount++
		if attemptCount < 3 {
			fmt.Printf("[Attempt %d] Network Timeout! Triggering Retryable Error...\n", attemptCount)
			return orderedjob.Retryable(errors.New("503 Service Unavailable"))
		}
		fmt.Printf("[Attempt %d] Success Calling HTTP Endpoint!\n", attemptCount)
		return nil
	})

	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{
		ChainID:  "http-chain",
		Sequence: 1,
		Type:     "HTTPCall",
	})

	time.Sleep(1 * time.Second)
}
```

---

## 6. Skenario 6: Worker Panic Recovery Isolation & Interceptor

### Fungsi Utama
Menangkap *unhandled runtime panic* (misal: *nil pointer dereference*) pada kode handler bisnis agar tidak menyebabkan worker pool atau proses aplikasi utama mengalami *crash*.

### Konfigurasi & Opsi Terlibat
-  `WithPanicHandler(func(ctx, job, panicVal, stack) error)`: Interceptor kustom untuk menangkap objek panic dan *stacktrace* mentah.
-  `WithRetryOnPanic(true)`: Mengizinkan percobaan ulang (retry) jika handler memicu panic.

### Efek Samping (*Side Effects*)
-  Jika `WithRetryOnPanic(false)` (default), panic akan langsung diubah menjadi status `FAILED` dengan *stack trace* lengkap dicatat pada kolom `last_error`.

### Contoh Kode Go

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

func main() {
	ctx := context.Background()
	repo := memory.New()

	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(2),
		orderedjob.WithPanicHandler(func(ctx context.Context, job orderedjob.Job, panicVal any, stack []byte) error {
			fmt.Printf(" [PANIC INTERCEPTED] JobID: %s | Panic: %v\n", job.ID, panicVal)
			return orderedjob.NonRetryable(fmt.Errorf("recovered panic: %v", panicVal))
		}),
		orderedjob.WithRetryOnPanic(false),
	)

	orderedjob.RegisterTyped(eng, "BadTask", func(ctx context.Context, job orderedjob.Job, payload map[string]string) error {
		var ptr *string
		fmt.Println(*ptr) // Nil pointer panic!
		return nil
	})

	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "panic-chain", Sequence: 1, Type: "BadTask"})

	time.Sleep(500 * time.Millisecond)
}
```

---

## 7. Skenario 7: Type-Safe Handlers & Struct Schema Validation

### Fungsi Utama
Melakukan validasi skema isi *payload* JSON sebelum handler dieksekusi. Jika payload tidak valid, job akan langsung ditolak sebelum menjalankan logika bisnis utama.

### Konfigurasi & Opsi Terlibat
1. **Interface `Validator`**: Implementasi metode `Validate() error` pada struct payload.
2. **Interface `ValidatorCtx`**: Implementasi metode `ValidateCtx(ctx) error` pada struct payload.
3. **`RegisterTypedWithValidator[T]`**: Menentukan fungsi validator kustom secara eksplisit saat pendaftaran.

### Contoh Kode Go

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

type UserRegistration struct {
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func (u UserRegistration) Validate() error {
	if u.Email == "" {
		return errors.New("email wajib diisi")
	}
	if u.Age < 17 {
		return errors.New("umur minimal 17 tahun")
	}
	return nil
}

func main() {
	ctx := context.Background()
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithConcurrency(2))

	orderedjob.RegisterTyped(eng, "RegisterUser", func(ctx context.Context, job orderedjob.Job, payload UserRegistration) error {
		fmt.Printf("Registrasi Berhasil: %s (Age: %d)\n", payload.Email, payload.Age)
		return nil
	})

	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{
		ChainID:  "reg-001",
		Sequence: 1,
		Type:     "RegisterUser",
		Payload:  UserRegistration{Email: "user@test.com", Age: 15},
	})

	time.Sleep(500 * time.Millisecond)
}
```

---

## 8. Skenario 8: Scheduled & Delayed Job Management

### Fungsi Utama
Menunda eksekusi pekerjaan ke titik waktu tertentu di masa depan (`AvailableAt`), atau mengubah waktu penjadwalan secara dinamis (`RescheduleJob`).

### Konfigurasi & Opsi Terlibat
-  `eng.Schedule(ctx, req, runAt)`: Menjadwalkan eksekusi tepat pada timestamp `runAt`.
-  `eng.EnqueueDelayed(ctx, req, delay)`: Menjadwalkan eksekusi setelah durasi `delay`.
-  `eng.RescheduleJob(ctx, jobID, newAvailableAt)`: Memperbarui timestamp `AvailableAt` dari job yang sudah ada.

### Efek Samping (*Side Effects*)
-  Job yang dijadwalkan di masa depan tidak akan diklaim oleh scanner sampai timestamp `AvailableAt <= NOW()`.

### Contoh Kode Go

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

func main() {
	ctx := context.Background()
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithConcurrency(2))

	orderedjob.RegisterTyped(eng, "Reminder", func(ctx context.Context, job orderedjob.Job, payload map[string]string) error {
		fmt.Printf(" [REMINDER FIRED] Text: %s at %s\n", payload["text"], time.Now().Format("15:04:05"))
		return nil
	})

	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	fmt.Printf("Waktu Enqueue: %s\n", time.Now().Format("15:04:05"))
	job, _ := eng.EnqueueDelayed(ctx, orderedjob.EnqueueRequest{
		ChainID:  "reminder-chain",
		Sequence: 1,
		Type:     "Reminder",
		Payload:  map[string]string{"text": "Bayar Tagihan Listrik"},
	}, 1*time.Second)

	// Ubah Penjadwalan (Reschedule) Tambah 500ms
	_, _ = eng.RescheduleJob(ctx, job.ID, time.Now().Add(1500*time.Millisecond))

	time.Sleep(2 * time.Second)
}
```

---

## 9. Skenario 9: Enterprise Observability (OpenTelemetry Tracing & Metrics)

### Fungsi Utama
Integrasi native dengan **OpenTelemetry (OTel)** dan **Prometheus** untuk penelusuran terdistribusi (*Distributed Tracing W3C TraceContext*) serta pengumpulan metrik performa antrean.

### Konfigurasi & Opsi Terlibat
-  `orderedjob.WithTraceID(ctx, "trace-id")`: Menyisipkan Trace ID pada penyerahan request.
-  `orderedjob.ExtractTraceID(ctx)`: Membaca Trace ID di dalam worker handler.
-  `orderedjob.NewOTelMetrics(meterProvider)`: Ekspor metrik standar OpenTelemetry/Prometheus.
-  `WithSlogLogger(slogLogger)`: Integrasi pencatatan log terstruktur `log/slog`.
-  `WithTracer(tracer)`: Mendaftarkan Tracer OpenTelemetry.

### Contoh Kode Go

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func main() {
	ctx := context.Background()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)))
	tracer := tp.Tracer("my-service")

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	repo := memory.New()
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(2),
		orderedjob.WithTracer(tracer),
		orderedjob.WithSlogLogger(logger),
	)

	orderedjob.RegisterTyped(eng, "AuditTask", func(ctx context.Context, job orderedjob.Job, payload map[string]string) error {
		traceID := orderedjob.ExtractTraceID(ctx)
		fmt.Printf("[HANDLER] TraceID: %s | Executing AuditTask\n", traceID)
		return nil
	})

	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	ctx, span := tracer.Start(ctx, "HTTP-Request-Handler")
	defer span.End()

	req := orderedjob.EnqueueRequest{
		ChainID:  "audit-1",
		Sequence: 1,
		Type:     "AuditTask",
		Payload:  map[string]string{"action": "LOGIN"},
	}

	_, _ = eng.Enqueue(ctx, req)
	time.Sleep(500 * time.Millisecond)
}
```

---

## 10. Skenario 10: Event Callback Listeners & HTTP Webhook Dispatcher

### Fungsi Utama
Mendengarkan *event lifecycle* antrean (seperti kegagalan job atau rantai yang terblokir) untuk memicu pemanggilan webhook HTTP atau alert Slack secara otomatis.

### Konfigurasi & Opsi Terlibat
-  `WithEventListener(customListener)`: Callback interface bawaan.
-  `WithDLQThreshold(10)`: Memicu event `OnDLQThresholdExceeded` jika job gagal mencapai 10.
-  `WithWebhook(WebhookConfig{URL, Secret, Timeout})`: Dispatcher notifikasi webhook HTTP otomatis.

### Contoh Kode Go

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

type CustomEventListener struct{}

func (l CustomEventListener) OnJobClaimed(job orderedjob.Job) {}
func (l CustomEventListener) OnJobCompleted(job orderedjob.Job, duration time.Duration) {}
func (l CustomEventListener) OnJobFailed(job orderedjob.Job, err error) {
	fmt.Printf(" [ALERT] Job %s pada Chain %s GAGAL: %v\n", job.ID, job.ChainID, err)
}
func (l CustomEventListener) OnJobRetrying(job orderedjob.Job, attempt int, nextAvailable time.Time) {}
func (l CustomEventListener) OnChainBlocked(chainID string, blockedJob orderedjob.Job) {
	fmt.Printf(" [CHAIN BLOCKED] Chain %s terhenti akibat Job %s!\n", chainID, blockedJob.ID)
}
func (l CustomEventListener) OnDLQThresholdExceeded(dlqCount int64) {
	fmt.Printf(" [DLQ ALERT] Jumlah job gagal mencapai ambang batas: %d\n", dlqCount)
}

func main() {
	ctx := context.Background()
	repo := memory.New()

	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(2),
		orderedjob.WithDLQThreshold(5),
		orderedjob.WithEventListener(CustomEventListener{}),
		orderedjob.WithWebhook(orderedjob.WebhookConfig{
			URL:     "https://hooks.slack.com/services/xxx/yyy/zzz",
			Timeout: 3 * time.Second,
		}),
	)

	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)
}
```

---

## 11. Skenario 11: Manajemen DLQ Lanjutan & Bulk Operations API

### Fungsi Utama
Menyediakan operasi masal (*Bulk Operations*) untuk mengelola job gagal pada Dead Letter Queue (DLQ) tanpa perlu memanipulasi database secara manual.

### Fitur & Operasi API
-  `eng.ReplayJob(ctx, jobID)`: Mencoba ulang 1 job gagal.
-  `eng.SkipJob(ctx, jobID)`: Menandai 1 job gagal sebagai `COMPLETED` untuk membuka unblock `Sequence N+1`.
-  `eng.BulkReplayDLQ(ctx, filter)`: Memulai *replay* masal berdasarkan kriteria filter.
-  `eng.BulkSkipDLQ(ctx, filter)`: Menandai *skip* masal untuk ribuan job gagal.
-  `eng.BulkPurgeDLQ(ctx, filter)`: Menghapus masal job gagal lama dari DLQ.
-  `eng.ReplayJobWithPayload(ctx, jobID, newJSONPayload)`: Memperbaiki isi payload JSON yang salah sebelum di-replay.

### Contoh Kode Go

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
)

func main() {
	ctx := context.Background()
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithConcurrency(2))

	// Replay Masal Seluruh Job Berstatus FAILED pada JobType "ProcessPayment"
	replayedCount, err := eng.BulkReplayDLQ(ctx, orderedjob.JobFilter{
		JobType: "ProcessPayment",
		Status:  orderedjob.StatusFailed,
	})
	if err == nil {
		fmt.Printf("[DLQ REPLAY] Berhasil memicu replay untuk %d jobs\n", replayedCount)
	}

	// Purge Masal Job Gagal yang Lebih Tua dari 30 Hari
	purgedCount, err := eng.BulkPurgeDLQ(ctx, orderedjob.JobFilter{
		CreatedBefore: time.Now().Add(-30 * 24 * time.Hour),
		Status:        orderedjob.StatusFailed,
	})
	if err == nil {
		fmt.Printf("[DLQ PURGE] Berhasil membersihkan %d jobs lama\n", purgedCount)
	}
}
```

---

## 12. Skenario 12: Web UI Dashboard & Real-Time SSE Streaming

### Fungsi Utama
Mengintegrasikan dashboard antarmuka web interaktif (`/ui`) ke dalam server HTTP Go milik Anda dengan pembaruan grafik dan statistik real-time berbasis **Server-Sent Events (SSE)**.

### Endpoint UI & Options
-  `ui.New(eng)`: Menginstansiasi HTTP handler dashboard Web UI.
-  `ui.WithPrefix("/admin/queue")`: Mengubah prefix path URL rute Web UI.
-  Endpoint `/ui/events`: Stream SSE real-time.

### Contoh Kode Go

```go
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
	"github.com/semmidev/orderedjob/ui"
)

func main() {
	ctx := context.Background()
	repo := memory.New()
	eng := orderedjob.New(repo, orderedjob.WithConcurrency(5))
	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	// Inisialisasi Router Web UI Dashboard
	uiHandler := ui.New(eng, ui.WithPrefix("/ui"))

	mux := http.NewServeMux()
	mux.Handle("/ui/", uiHandler)

	fmt.Println(" Web UI Dashboard berjalan di http://localhost:8080/ui/")
	_ = http.ListenAndServe(":8080", mux)
}
```

---

## 13. Skenario 13: End-to-End Enterprise Master Pipeline (Semua Fitur Digabung)

### Fungsi Utama
Cetak birut aplikasi *production-ready* kelas *enterprise* yang menggabungkan seluruh fitur `orderedjob`: Multi-DB PostgreSQL migration, LISTEN/NOTIFY, OpenTelemetry Tracing, slog logger, panic isolation guard, auto-sequence, idempotency key, webhook alert, dan SSE Web UI Dashboard.

### Contoh Kode Go Lengkap

```go
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/semmidev/orderedjob"
	pgRepo "github.com/semmidev/orderedjob/repository/postgres"
	"github.com/semmidev/orderedjob/retry"
	"github.com/semmidev/orderedjob/ui"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type MasterOrderPayload struct {
	OrderID       string  `json:"order_id"`
	TotalAmount   float64 `json:"total_amount"`
	CustomerEmail string  `json:"customer_email"`
}

func (m MasterOrderPayload) Validate() error {
	if m.OrderID == "" {
		return fmt.Errorf("order_id tidak boleh kosong")
	}
	if m.TotalAmount <= 0 {
		return fmt.Errorf("total_amount harus lebih dari 0")
	}
	return nil
}

func main() {
	ctx := context.Background()

	// 1. Slog Structured Logger Setup
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// 2. OpenTelemetry Setup
	tp := sdktrace.NewTracerProvider()
	tracer := tp.Tracer("enterprise-pipeline")

	// 3. Connection Pool PostgreSQL & Migrasi
	connStr := "postgres://postgres:postgres@localhost:5432/orderedjob_db?sslmode=disable"
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Koneksi DB Gagal: %v", err)
	}
	defer pool.Close()

	repo := pgRepo.New(pool)
	if err := repo.Migrate(ctx); err != nil {
		log.Fatalf("Migrasi Skema Gagal: %v", err)
	}

	// 4. Inisialisasi Engine Kelas Enterprise
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(20),
		orderedjob.WithPollInterval(200*time.Millisecond),
		orderedjob.WithLease(45*time.Second),
		orderedjob.WithRecoveryInterval(10*time.Second),
		orderedjob.WithWorkerID("master-worker-node-1"),
		orderedjob.WithNotify(true),
		orderedjob.WithSlogLogger(logger),
		orderedjob.WithTracer(tracer),
		orderedjob.WithDLQThreshold(10),
		orderedjob.WithRetryPolicy(retry.Policy{
			MaxAttempts: 5,
			BaseDelay:   500 * time.Millisecond,
			MaxDelay:    30 * time.Second,
			Jitter:      retry.FullJitter,
		}),
		orderedjob.WithPanicHandler(func(ctx context.Context, job orderedjob.Job, panicVal any, stack []byte) error {
			logger.Error("Master panic caught", "job_id", job.ID, "panic", panicVal)
			return orderedjob.NonRetryable(fmt.Errorf("panic: %v", panicVal))
		}),
		orderedjob.WithWebhook(orderedjob.WebhookConfig{
			URL:     "https://api.mycompany.com/webhooks/queue-alerts",
			Timeout: 5 * time.Second,
		}),
	)

	// 5. Daftarkan Handler Type-Safe
	orderedjob.RegisterTyped(eng, "ProcessMasterOrder", func(ctx context.Context, job orderedjob.Job, payload MasterOrderPayload) error {
		traceID := orderedjob.ExtractTraceID(ctx)
		logger.Info("Memproses Master Order", "order_id", payload.OrderID, "trace_id", traceID, "seq", job.Sequence)
		return nil
	})

	// 6. Jalankan Engine
	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Gagal Start Engine: %v", err)
	}
	defer eng.Shutdown(ctx)

	// 7. Mount Web UI Dashboard HTTP Server
	mux := http.NewServeMux()
	mux.Handle("/ui/", ui.New(eng))
	go func() {
		logger.Info("Web UI Dashboard aktif di http://localhost:8080/ui/")
		_ = http.ListenAndServe(":8080", mux)
	}()

	// 8. Enqueue Transaksi dengan Idempotency Key & Trace Context
	ctx, span := tracer.Start(ctx, "HTTP-CreateOrder")
	defer span.End()

	req := orderedjob.EnqueueRequest{
		ChainID:        "tenant-acme-order-1001",
		Sequence:       0, // Auto-Sequence
		Type:           "ProcessMasterOrder",
		Payload:        MasterOrderPayload{OrderID: "ORD-1001", TotalAmount: 1250.50, CustomerEmail: "buyer@acme.com"},
		IdempotencyKey: "idemp-trx-ord-1001",
		TenantID:       "tenant-acme",
	}

	job, err := eng.Enqueue(ctx, req)
	if err != nil {
		logger.Error("Enqueue Gagal", "error", err)
	} else {
		logger.Info("Enqueue Berhasil", "job_id", job.ID, "sequence", job.Sequence)
	}

	// Biarkan aplikasi berjalan
	select {}
}
```
