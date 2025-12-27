# Synaxis

Sistem Sinkronisasi Terdistribusi yang mengimplementasikan Raft consensus, distributed queue dengan consistent hashing, dan protokol MESI cache coherence.

## Fitur Utama

- **Distributed Lock Manager**: Raft consensus, shared/exclusive locks, deadlock detection
- **Distributed Queue**: Consistent hashing, partitions, at-least-once delivery
- **Cache Coherence**: Protokol MESI, LRU replacement, bus transactions
- **PBFT Bonus**: Byzantine fault tolerance (dapat diaktifkan)

## Quick Start

```bash
cp .env.example .env
docker compose -f docker/docker-compose.yml up -d
```

## Arsitektur

Lihat [docs/architecture.md](docs/architecture.md) untuk diagram lengkap.

## Referensi API

Lihat [docs/api_spec.yaml](docs/api_spec.yaml) untuk spesifikasi OpenAPI.

## Development

```bash
uv sync
uv run pytest tests/ -v
uv run basedpyright src/
uv run ruff check src/
```

## Load Testing

```bash
uv run locust -f benchmarks/load_test_scenarios.py --headless -u 50 -r 10 -t 60s
```

## Lisensi

MIT
