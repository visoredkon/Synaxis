# Panduan Deployment

## Prasyarat

- Docker 24.0+
- Docker Compose 2.20+
- Python 3.12+ (untuk development lokal)
- uv (Python package manager)

---

## Quick Start

### 1. Clone dan Setup

```bash
cd Synaxis
cp .env.example .env
```

### 2. Jalankan Semua Services

```bash
docker compose -f docker/docker-compose.yml up -d
```

Ini akan menjalankan:
- 1 instance Redis
- 3 Lock Manager nodes (port 8000-8002)
- 3 Queue nodes (port 8100-8102)
- 3 Cache nodes (port 8200-8202)

### 3. Verifikasi Kesehatan Cluster

```bash
curl http://localhost:8000/health
curl http://localhost:8100/health
curl http://localhost:8200/health
```

---

## Konfigurasi

### Environment Variables

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| `NODE_ID` | node-1 | Identifier unik node |
| `NODE_PORT` | 8000 | Port HTTP server |
| `REDIS_URL` | redis://localhost:6379 | URL koneksi Redis |
| `CLUSTER_NODES` | - | Daftar node dipisah koma |
| `CONSENSUS_TYPE` | raft | Algoritma consensus (raft/pbft) |
| `ELECTION_TIMEOUT_MIN` | 150 | Min election timeout (ms) |
| `ELECTION_TIMEOUT_MAX` | 300 | Max election timeout (ms) |
| `LOG_LEVEL` | INFO | Level logging |

### Beralih ke PBFT

```bash
export CONSENSUS_TYPE=pbft
docker compose -f docker/docker-compose.yml up -d
```

Atau di `.env`:
```env
CONSENSUS_TYPE=pbft
```

---

## Scaling

### Menambah Lock Manager Nodes

```bash
docker compose -f docker/docker-compose.yml up -d --scale lock-manager=5
```

### Menambah Queue Partitions

Update `docker-compose.yml`:
```yaml
environment:
  - NUM_PARTITIONS=6
```

---

## Monitoring

### Aktifkan Prometheus & Grafana

```bash
docker compose -f docker/docker-compose.yml --profile monitoring up -d
```

Akses:
- Prometheus: http://localhost:9999
- Grafana: http://localhost:3000 (admin/admin)

### Metrics yang Tersedia

| Metric | Deskripsi |
|--------|-----------|
| `synaxis_requests_total` | Total requests berdasarkan tipe |
| `synaxis_request_latency_seconds` | Histogram latency request |
| `synaxis_active_locks` | Jumlah active locks saat ini |
| `synaxis_queue_size` | Ukuran queue berdasarkan topic |
| `synaxis_cache_hits_total` | Jumlah cache hit |
| `synaxis_consensus_term` | Term Raft saat ini |

---

## Development Lokal

### Install Dependencies

```bash
uv sync
```

### Jalankan Single Node

```bash
NODE_TYPE=lock_manager NODE_ID=dev-1 NODE_PORT=8000 uv run python -m src.main
```

### Jalankan Tests

```bash
uv run pytest tests/ -v
uv run basedpyright src/
uv run ruff check src/
```

### Load Testing

```bash
uv run locust -f benchmarks/load_test_scenarios.py --headless -u 50 -r 10 -t 60s
```

---

## Troubleshooting

### Node Tidak Bergabung ke Cluster

1. Pastikan `CLUSTER_NODES` sesuai dengan alamat node yang aktual
2. Verifikasi konektivitas jaringan antar container
3. Pastikan Redis dapat diakses

### Leader Election Terhenti

1. Tingkatkan nilai `ELECTION_TIMEOUT_MAX`
2. Periksa network partitions
3. Pastikan jumlah node ganjil

### Cache Miss Rate Tinggi

1. Tingkatkan ukuran cache via env `CACHE_SIZE`
2. Periksa pola invalidation
3. Verifikasi rasio write/read

### Logs

```bash
docker compose -f docker/docker-compose.yml logs -f lock-manager-1
docker compose -f docker/docker-compose.yml logs -f queue-1
```
