<div align="center">

  <h1>⚡ OrderedJob</h1>
  <p><b>High-Throughput, PostgreSQL-Backed Ordered Job Engine for Go</b></p>

  <p>
    <a href="https://pkg.go.dev/github.com/semmidev/orderedjob"><img src="https://img.shields.io/badge/go.dev-reference-007d9c?style=for-the-badge&logo=go&logoColor=white" alt="GoDoc" /></a>
    <a href="https://github.com/semmidev/orderedjob/releases"><img src="https://img.shields.io/github/v/tag/semmidev/orderedjob?color=00ADD8&label=release&style=for-the-badge&logo=github" alt="Release" /></a>
    <a href="https://github.com/semmidev/orderedjob/actions"><img src="https://img.shields.io/github/actions/workflow/status/semmidev/orderedjob/ci.yml?branch=main&label=CI&style=for-the-badge&logo=github-actions" alt="CI" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green?style=for-the-badge" alt="License" /></a>
    <a href="https://www.postgresql.org/"><img src="https://img.shields.io/badge/PostgreSQL-13%2B-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL" /></a>
  </p>

  <p>
    <code>github.com/semmidev/orderedjob</code> mengeksekusi job secara berurutan (<b>FIFO per chain</b>) dengan eksekusi paralel antar-chain.<br/>PostgreSQL digunakan sebagai storage engine untuk locking, ordering, retry, dan recovery.
  </p>

</div>

---

## 📌 Daftar Isi

- [📦 Instalasi](#-instalasi)
- [✨ Fitur Utama](#-fitur-utama)
- [💡 Contoh Usecase](#-contoh-usecase)
- [🏗️ Arsitektur](#️-arsitektur)
- [🚦 State Machine](#-state-machine)
- [🚀 Contoh Penggunaan](#-contoh-penggunaan)
- [🛠️ Penanganan Job Gagal (DLQ Operations)](#️-penanganan-job-gagal-dlq-operations)
- [⚙️ Opsi Konfigurasi Engine](#️-opsi-konfigurasi-engine)
- [🧪 Testing](#-testing)
- [📄 Lisensi](#-lisensi)

---

## 📦 Instalasi

```bash
go get github.com/semmidev/orderedjob
```

---

## ✨ Fitur Utama

- 🔒 **Per-Chain Ordering**: Job $N$ hanya berjalan setelah Job $N-1$ berstatus `COMPLETED`.
- 🚀 **Paralelisme Antar Chain**: Ribuan chain berjalan bersamaan secara independen tanpa lock contention.
- 🔔 **Instant Event Notification**: Menggunakan PostgreSQL `LISTEN/NOTIFY` untuk latensi klaim `< 5ms` tanpa busy-polling.
- 🎯 **Type-Safe Handlers**: Pendaftaran handler menggunakan Go Generics (`RegisterTyped[T]`) dengan unmarshaling JSON otomatis.
- 🛡️ **Fencing Protection**: Fencing token (`lease_generation`) mencegah race condition dari worker yang terhambat (*lagging*).
- 🛠️ **Manajemen DLQ**: Menyediakan API `ReplayJob` dan `SkipJob` untuk memulihkan atau melewati job yang gagal.
- 🌐 **Distributed Tracing**: Dukungan *TraceContext propagation* (`TraceID`) antar pemanggilan asinkron.
- 🗄️ **Multi-Database Support**: Dukungan penuh persistence engine untuk **PostgreSQL** (`pgxpool`) dan **Microsoft SQL Server** (T-SQL `UPDLOCK, READPAST`).
- 🔌 **In-Memory Adapter**: Adapter `memory` bawaan untuk pengujian cepat tanpa database.

---

## 💡 Contoh Usecase

- **✅ Multi-Stage Approval Workflow**: Memproses alur persetujuan bertahap (*Approve A* ➔ *Approve B* ➔ ... ➔ *Approve E*) secara sekuensial per dokumen/permohonan dengan pengiriman API inter-service otomatis.
- **💳 Pipeline Transaksi Finansial**: Menjamin siklus pembayaran (*Validate Account* ➔ *Hold Balance* ➔ *Transfer* ➔ *Send Receipt*) berjalan tepat berurutan per akun tanpa race condition.
- **📦 Pemrosesan Pesanan E-Commerce**: Memproses tahapan pesanan (*Create Order* ➔ *Deduct Inventory* ➔ *Generate Invoice* ➔ *Ship Package*) sesuai urutan per pesanan.
- **👤 User Onboarding Workflow**: Eksekusi berurutan untuk registrasi pengguna (*Create User* ➔ *Send Verification Email* ➔ *Provision Default Workspace* ➔ *Trigger Analytics Event*).
- **🔄 Event-Driven State Machines**: Mengolah stream kejadian berurutan (*CDC / Event Sourcing*) yang membutuhkan jaminan eksekusi FIFO per entity ID atau tenant ID.
- **🏢 SaaS Multi-Tenant Background Jobs**: Menjalankan *background processing* berat dengan isolasi antar tenant tanpa memblokir pemrosesan tenant lain.

---

## 🏗️ Arsitektur

<div align="center">

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

</div>

---

## 🚦 State Machine

<div align="center">

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

</div>

> [!IMPORTANT]
> Hanya status `COMPLETED` yang melepaskan urutan job berikutnya (*sequence N+1*). Status `FAILED`, `CANCELLED`, atau `EXPIRED` akan memblokir chain secara default demi keamanan data.

---

## 🚀 Contoh Penggunaan

Untuk melihat contoh kode aplikasi lengkap yang mencakup integrasi PostgreSQL, pencatatan log terstruktur (`log/slog`), metrik Prometheus / OpenTelemetry, penanganan error retryable, enkui batch satu transaksi DB, dan propagasi context tracing, silakan kunjungi folder **[example/](example/)**.

- 📖 **[Dokumentasi & Cara Menjalankan Example](example/README.md)**
- 💻 **[Kode Sumber Aplikasi Percontohan (`example/main.go`)](example/main.go)**

---

## 🛠️ Penanganan Job Gagal (DLQ Operations)

Jika sebuah job berstatus `FAILED`, chain akan terhenti sampai ada penanganan manual:

### Replay Job
Mengembalikan job `FAILED` atau `CANCELLED` ke status `PENDING` untuk dicoba ulang:

```go
err := eng.ReplayJob(ctx, failedJobID)
```

### Skip Job
Menandai job `FAILED` sebagai `COMPLETED` agar urutan berikutnya dapat dilanjutkan:

```go
err := eng.SkipJob(ctx, failedJobID)
```

---

## ⚙️ Opsi Konfigurasi Engine

| Option | Deskripsi | Default |
| :--- | :--- | :--- |
| `WithConcurrency(n)` | Jumlah worker goroutine | `10` |
| `WithPollInterval(d)` | Interval polling fallback (dengan random jitter) | `500ms` |
| `WithLease(d)` | Durasi sewa job per worker (heartbeat = lease/3) | `60s` |
| `WithRetryPolicy(p)` | Konfigurasi backoff retry (`MaxAttempts`, `BaseDelay`, `MaxDelay`, `Jitter`) | `Max 5, Base 1s, Max 5m` |
| `WithSlogLogger(l)` | Integrasi `log/slog` | `NoopLogger` |
| `WithMetrics(m)` | Integrasi Prometheus / OpenTelemetry Metrics | `NoopMetrics` |

---

## 🧪 Testing

Gunakan adapter `memory` untuk unit testing tanpa database:

```go
import "github.com/semmidev/orderedjob/repository/memory"

repo := memory.New()
eng := orderedjob.New(repo, orderedjob.WithConcurrency(5))
```

Menjalankan test dan linter:

```bash
make test
make lint
```

---

## 📄 Lisensi

[MIT License](LICENSE)
