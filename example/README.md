# OrderedJob Example Application

Aplikasi contoh yang menunjukkan penggunaan lengkap pustaka `orderedjob` dengan database persistence PostgreSQL.

## Prasyarat

- **Go 1.22+**
- **Docker & Docker Compose** (atau PostgreSQL 14+ lokal)

## Cara Menjalankan

### 1. Jalankan PostgreSQL

Gunakan Docker Compose untuk menjalankan instance PostgreSQL:

```bash
docker compose up -d
```

Perintah di atas akan menjalankan PostgreSQL pada port `5432` dengan kredensial bawaan:
- **Database**: `orderedjob`
- **User**: `postgres`
- **Password**: `postgres`

### 2. Jalankan Program Contoh

Jalankan file `main.go`:

```bash
go run main.go
```

Jika kamu menggunakan instance PostgreSQL lain, tentukan variabel lingkungan `DATABASE_URL`:

```bash
DATABASE_URL="postgres://username:password@localhost:5432/my_db?sslmode=disable" go run main.go
```

### 3. Hentikan PostgreSQL

Untuk menghentikan dan menghapus container serta volume PostgreSQL:

```bash
docker compose down -v
```

---

## Fitur-Fitur Utama yang Ditunjukkan (`main.go`)

Program contoh ini mencakup demonstrasi dari seluruh kemampuan pustaka `orderedjob`:

### 1. Persistence & Otomatisasi Skema
- **Auto Migration**: Menjalankan migrasi DDL tabel `ordered_jobs` secara otomatis melalui `repo.Migrate(ctx)`.
- **PGX Connection Pooling**: Menggunakan `pgxpool.Pool` untuk performa kueri database yang optimal.

### 2. Konfigurasi Engine & Worker Pool
- **`WithConcurrency(5)`**: Menjalankan 5 worker goroutine secara paralel.
- **`WithPollInterval(100ms)`**: Interval polling job yang responsif.
- **`WithLease(15s)`**: Batas waktu kepemilikan lock job sebelum dianggap lelap (*stale*).
- **`WithSlogLogger(logger)`**: Integrasi terstruktur dengan `log/slog` bawaan Go.
- **`WithNotify(true)`**: Bangunkan worker secara cepat saat job baru datang via PostgreSQL `LISTEN/NOTIFY`.
- **`WithRetryPolicy(...)`**: Strategi percobaan ulang (*retry*) berbasis exponential backoff dengan jitter.

### 3. Pendaftaran Handler (Type-Safe Generics & Dynamic)
- **Type-Safe Handlers (`RegisterTyped[T]`)**: Pendaftaran handler dengan deserialisasi payload JSON otomatis berbasis Go Generics untuk `OrderPayload` dan `NotificationPayload`.
- **Dynamic Raw Handler (`RegisterFunc`)**: Pendaftaran handler langsung menggunakan payload `json.RawMessage` (contoh: `AuditLog`).

### 4. Distirbuted Tracing (Trace Context Propagation)
- **`WithTraceID(ctx, traceID)`**: Menyisipkan trace ID ke dalam context saat enqueue.
- **`ExtractTraceID(ctx)`**: Membaca trace ID dari context di dalam handler untuk korelasi log end-to-end.

### 5. Penanganan Error Transien vs Permanen
- **`orderedjob.Retryable(err)`**: Menandai error transien (misal: timeout jaringan) agar worker melakukan retry sesuai kebijakan backoff.
- **Error Non-Retryable**: Jika error biasa dikembalikan, job langsung dipindahkan ke status terminal/DLQ tanpa retry tak terbatas.

### 6. Mode Enkui & Jaminan Urutan FIFO
- **Explicit Sequence FIFO**: Menjamin urutan eksekusi pekerjaan tepat sesuai indeks urutan (`Sequence = 1, 2, 3...`) pada tiap chain isolasi.
- **Single-Transaction Batch Enqueue (`EnqueueBatch`)**: Memasukkan banyak job sekaligus dalam 1 transaksi database yang atomic.
- **Auto-Sequence (`Sequence = 0`)**: Menggenerasi indeks urutan berikutnya secara otomatis di sisi database.

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

### Contoh Adapter Prometheus (`client_golang`)

Untuk mengintegrasikan `orderedjob` dengan Prometheus di aplikasi produksi, kamu cukup membuat struct adapter sederhana:

```go
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/semmidev/orderedjob"
)

type PrometheusMetrics struct {
	claimed        prometheus.Counter
	completed      *prometheus.CounterVec
	failed         *prometheus.CounterVec
	retries        *prometheus.CounterVec
	execDuration   *prometheus.HistogramVec
	queueDelay     *prometheus.HistogramVec
	activeLeases   prometheus.Gauge
	claimConflicts prometheus.Counter
	staleRecovered prometheus.Counter
	blockedChains  prometheus.Counter
}

func NewPrometheusMetrics(reg prometheus.Registerer) *PrometheusMetrics {
	m := &PrometheusMetrics{
		claimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "orderedjob_jobs_claimed_total",
			Help: "Total number of jobs claimed by workers",
		}),
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderedjob_jobs_completed_total",
			Help: "Total number of completed jobs",
		}, []string{"job_type"}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderedjob_jobs_failed_total",
			Help: "Total number of failed jobs",
		}, []string{"job_type"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderedjob_job_retries_total",
			Help: "Total number of job retries",
		}, []string{"job_type"}),
		execDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "orderedjob_job_execution_duration_seconds",
			Help:    "Execution duration of jobs in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"job_type"}),
		queueDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "orderedjob_job_queue_delay_seconds",
			Help:    "Queue delay duration in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"job_type"}),
		activeLeases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "orderedjob_active_leases",
			Help: "Current active leases held by worker",
		}),
		claimConflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "orderedjob_claim_conflicts_total",
			Help: "Total claim conflicts encountered",
		}),
		staleRecovered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "orderedjob_stale_recovered_total",
			Help: "Total stale leases recovered",
		}),
		blockedChains: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "orderedjob_blocked_chains_total",
			Help: "Total blocked chains encountered",
		}),
	}

	reg.MustRegister(
		m.claimed, m.completed, m.failed, m.retries,
		m.execDuration, m.queueDelay, m.activeLeases,
		m.claimConflicts, m.staleRecovered, m.blockedChains,
	)
	return m
}

func (p *PrometheusMetrics) IncClaimed()                                         { p.claimed.Inc() }
func (p *PrometheusMetrics) IncCompleted(jobType string)                         { p.completed.WithLabelValues(jobType).Inc() }
func (p *PrometheusMetrics) IncFailed(jobType string)                            { p.failed.WithLabelValues(jobType).Inc() }
func (p *PrometheusMetrics) IncRetry(jobType string)                             { p.retries.WithLabelValues(jobType).Inc() }
func (p *PrometheusMetrics) ObserveExecDuration(jobType string, d time.Duration) { p.execDuration.WithLabelValues(jobType).Observe(d.Seconds()) }
func (p *PrometheusMetrics) ObserveQueueDelay(jobType string, d time.Duration)   { p.queueDelay.WithLabelValues(jobType).Observe(d.Seconds()) }
func (p *PrometheusMetrics) SetActiveLeases(n int)                               { p.activeLeases.Set(float64(n)) }
func (p *PrometheusMetrics) IncClaimConflicts()                                  { p.claimConflicts.Inc() }
func (p *PrometheusMetrics) IncStaleRecovered(n int)                             { p.staleRecovered.Add(float64(n)) }
func (p *PrometheusMetrics) IncBlockedChain()                                    { p.blockedChains.Inc() }
```
