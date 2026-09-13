# orderedjob — High-Throughput Ordered Job Engine for Go

`github.com/semmidev/orderedjob` adalah library Go bertingkat enterprise yang menyediakan **strict per-chain ordering** dengan concurrency tinggi antar chain. PostgreSQL bertindak sebagai *single source of truth* untuk state management, ordering, locking, automatic retry, stale lease recovery, dan auditability.

---

## 🌟 Fitur Utama

- 🔒 **Strict Per-Chain Ordering**: Guaranteed FIFO execution per `chain_id`. Job $N$ tidak akan dieksekusi sebelum Job $N-1$ berstatus `COMPLETED` (C1 Invariant).
- 🚀 **High-Throughput Parallelism**: Ribuan `chain_id` dapat dieksekusi secara independen dan paralel antar worker tanpa *lock contention*.
- 🔔 **Instant LISTEN/NOTIFY Event Stream**: Latensi klaim job `< 5ms` menggunakan PostgreSQL push-based notifications tanpa *busy-polling*.
- 🎯 **Type-Safe Handlers dengan Go Generics**: Daftarkan handler menggunakan `RegisterTyped[T]` dengan unmarshaling JSON otomatis tanpa *boilerplate code*.
- 🛡️ **Fencing Token (`lease_generation`)**: Mencegah *stale worker* atau *lagging worker* menimpa hasil kerja worker baru (*Fencing Violation Protection*).
- 🛠️ **Dead Letter Queue (DLQ) & Incident Ops**: API bawaan `ReplayJob` dan `SkipJob` untuk manajemen kegagalan jaringan atau insiden produksi.
- 🌐 **OpenTelemetry (OTel) Tracing Context**: Integrasi *W3C Trace Context propagation* dari enqueue awal hingga eksekusi asinkron di worker pool.
- 🔌 **Hexagonal Architecture (Ports & Adapters)**: Tersedia implementasi `postgres` untuk produksi dan `memory` untuk unit testing cepat tanpa database.

---

## 🏗️ Arsitektur

```mermaid
graph TD
    Client["Client App / Service"] -->|"Enqueue(req)"| DB[("PostgreSQL\n(ordered_jobs)")]
    
    subgraph Engine["OrderedJob Engine"]
        Listen["LISTEN Stream\n(< 5ms Latency)"]
        Scanner["SKIP LOCKED Scanner\n(Partial Index)"]
        WorkerPool["Worker Pool\n(Goroutines)"]
        Registry["Handler Registry\n(Type-Safe Generics)"]
        Recovery["Recovery Worker\n(Stale Lease Recovery)"]
    end

    DB -->|"pg_notify"| Listen
    DB -->|"FOR UPDATE SKIP LOCKED"| Scanner
    Listen -->|"Wakeup Event"| WorkerPool
    Scanner -->|"Claim Job"| WorkerPool
    Recovery -->|"Reclaim Stale Jobs"| DB
    WorkerPool -->|"Execute"| Registry
    Registry -->|"Side Effects / Actions"| External["External Systems / APIs"]
```

---

## 🚦 State Machine & Transisi Job

```mermaid
stateDiagram-v2
    [*] --> BLOCKED: Sequence > 1 (Predecessor Pending)
    [*] --> PENDING: Sequence = 1 / Auto Sequence
    
    BLOCKED --> PENDING: Predecessor Status = COMPLETED
    PENDING --> PROCESSING: Worker Claim (Acquire Lease)
    RETRYING --> PROCESSING: Worker Claim (Acquire Lease)
    
    PROCESSING --> COMPLETED: Handler Success (Promote Next Sequence)
    PROCESSING --> RETRYING: Transient Failure (Backoff & Jitter)
    PROCESSING --> FAILED: Terminal Error / Max Attempts Reached
    PROCESSING --> CANCEL_REQUESTED: User Cancel Request
    
    CANCEL_REQUESTED --> CANCELLED: Worker Confirm Cancel
    FAILED --> DEAD_LETTERED: Final DLQ State
    
    COMPLETED --> [*]
    CANCELLED --> [*]
    DEAD_LETTERED --> [*]
```

> [!IMPORTANT]
> Hanya status `COMPLETED` yang melepaskan urutan job berikutnya (*sequence N+1*). Status `FAILED`, `CANCELLED`, atau `EXPIRED` akan memblokir chain secara default demi keamanan integritas data.

---

## ⚡ Quick Start

### 1. Jalankan PostgreSQL

Gunakan Docker Compose dari folder `example/`:

```bash
cd example
docker compose up -d
```

### 2. Kode Aplikasi (`main.go`)

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/semmidev/orderedjob"
	pgRepo "github.com/semmidev/orderedjob/repository/postgres"
)

type PaymentPayload struct {
	AccountID string  `json:"account_id"`
	Amount    float64 `json:"amount"`
}

func main() {
	ctx := context.Background()
	
	// 1. Inisialisasi Database Pool & Migration
	pool, _ := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/orderedjob?sslmode=disable")
	repo := pgRepo.New(pool)
	_ = repo.Migrate(ctx)

	// 2. Buat Engine & Konfigurasi
	eng := orderedjob.New(repo,
		orderedjob.WithConcurrency(10),
		orderedjob.WithPollInterval(200*time.Millisecond),
		orderedjob.WithLease(30*time.Second),
	)

	// 3. Register Type-Safe Handler (Generics)
	orderedjob.RegisterTyped(eng, "ProcessPayment", func(ctx context.Context, job orderedjob.Job, payload PaymentPayload) error {
		fmt.Printf("[worker] chain=%s seq=%d account=%s amount=%.2f trace=%s\n",
			job.ChainID, job.Sequence, payload.AccountID, payload.Amount, job.TraceID)
		return nil
	})

	// 4. Jalankan Worker Pool
	_ = eng.Start(ctx)
	defer eng.Shutdown(ctx)

	// 5. Enqueue Job Berurutan (Sequential Chain)
	ctx = orderedjob.WithTraceID(ctx, "trace-id-12345")

	for seq := int64(1); seq <= 3; seq++ {
		_, _ = eng.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "acc-8899",
			Sequence: seq,
			Type:     "ProcessPayment",
			Payload:  PaymentPayload{AccountID: "acc-8899", Amount: float64(seq * 100)},
		})
	}

	time.Sleep(2 * time.Second)
}
```

---

## 🛠️ API Management & Incident Operations (DLQ)

Ketika sebuah job mengalami kegagalan permanen (`FAILED`), chain tersebut akan terblokir untuk mencegah eksekusi beruntun data yang bermasalah. Anda dapat mengelolanya melalui API berikut:

### 1. Replay Job (`ReplayJob`)
Mengembalikan status job `FAILED` atau `CANCELLED` menjadi `PENDING` agar dapat dicoba kembali oleh worker pool setelah *bugfix* diterapkan:

```go
err := eng.ReplayJob(ctx, failedJobID)
```

### 2. Skip Job (`SkipJob`)
Menandai job `FAILED` sebagai `COMPLETED` untuk mengabaikan error dan melanjutkan eksekusi urutan berikutnya pada chain:

```go
err := eng.SkipJob(ctx, failedJobID)
```

---

## ⚙️ Konfigurasi Engine

| Option | Deskripsi | Default |
| :--- | :--- | :--- |
| `WithConcurrency(n)` | Jumlah worker goroutine eksekusi paralel | `10` |
| `WithPollInterval(d)` | Interval polling fallback (dilengkapi random jitter) | `500ms` |
| `WithLease(d)` | Durasi sewa kepemilikan job per worker (heartbeat = lease/3) | `60s` |
| `WithRetryPolicy(p)` | Strategi backoff retry (`MaxAttempts`, `BaseDelay`, `MaxDelay`, `Jitter`) | `Max 5, Base 1s, Max 5m` |
| `WithSlogLogger(l)` | Integrasi structured logging (`log/slog`) | `NoopLogger` |
| `WithMetrics(m)` | Integrasi Prometheus / OpenTelemetry Metrics | `NoopMetrics` |

---

## 🧪 Testing

Library ini menyediakan in-memory repository (`repository/memory`) untuk pengujian tanpa memerlukan instance PostgreSQL:

```go
import "github.com/semmidev/orderedjob/repository/memory"

repo := memory.New()
eng := orderedjob.New(repo, orderedjob.WithConcurrency(5))
```

Jalankan test suite dengan Go Race Detector:

```bash
make test
```

Jalankan linter:

```bash
make lint
```

---

## 📁 Package Layout

```text
├── engine.go           # Core orchestrator, worker loops, claim & heartbeat
├── repository.go       # Repository Port interface
├── handler.go          # Handler Registry & Generic Type-Safe Register helpers
├── observability.go    # Logger, Metrics interface & TraceID context propagation
├── job.go              # Job & EnqueueRequest domain models
├── errors.go           # Domain errors & Retryable/NonRetryable error wrappers
├── repository/
│   ├── postgres/       # PostgreSQL adapter (LISTEN/NOTIFY, SKIP LOCKED, Migrations)
│   └── memory/         # In-memory adapter (Mutex & Maps untuk Unit Testing)
├── retry/              # Retry Policy & Exponential Backoff Jitter
├── lease/              # Lease & Heartbeat Duration Manager
├── lifecycle/          # State machine constants & transition rules
└── example/            # Sample runnable application + Docker Compose
```

---

## 📄 Lisensi

[MIT License](LICENSE)
