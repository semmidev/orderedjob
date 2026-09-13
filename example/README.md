# OrderedJob Example Application

Contoh aplikasi sederhana yang menunjukkan penggunaan library `orderedjob` dengan persistence PostgreSQL.

## Prasyarat

- **Go 1.22+**
- **Docker** & **Docker Compose** (atau PostgreSQL lokal versi 14+)

## Cara Menjalankan

### 1. Jalankan PostgreSQL

Gunakan Docker Compose untuk menjalankan database PostgreSQL:

```bash
docker compose up -d
```

Command di atas akan menjalankan PostgreSQL container pada port `5432` dengan database `orderedjob`, user `postgres`, dan password `postgres`.

### 2. Jalankan Aplikasi

Jalankan program contoh:

```bash
go run main.go
```

Jika kamu menggunakan PostgreSQL instance lain, kamu bisa menggunakan environment variable `DATABASE_URL`:

```bash
DATABASE_URL="postgres://username:password@localhost:5432/my_db?sslmode=disable" go run main.go
```

### 3. Menghentikan PostgreSQL

Untuk menghentikan dan membersihkan container PostgreSQL:

```bash
docker compose down -v
```

---

## Apa yang Ditunjukkan pada Contoh Ini?

1. **Auto Migration**: Menjalankan skema tabel `ordered_jobs` otomatis via `repo.Migrate(ctx)`.
2. **Konfigurasi Engine**:
   - `WithConcurrency(10)`: Menjalankan 10 worker paralel.
   - `WithPollInterval(200ms)`: Polling interval dengan random jitter.
   - `WithLease(30s)`: Sewa kepemilikan job selama 30 detik.
   - `WithSlogLogger(logger)`: Penggunaan structured logger (`log/slog`).
   - `WithRetryPolicy(...)`: Strategi retry dengan exponential backoff dan jitter.
3. **Register Handler**: Mendaftarkan handler `ProcessOrder` yang bertindak secara *idempotent*.
4. **Enqueue Sequential Jobs**:
   - Menambahkan job berurutan (urutan 1 sampai 5) pada 3 chain terpisah (`order-1`, `order-2`, `order-3`).
   - Menambahkan job dengan penomoran urutan otomatis (*auto sequence*) pada chain `order-auto`.
5. **Graceful Shutdown**: Menghentikan worker pool dengan aman menggunakan `eng.Shutdown(ctx)`.
