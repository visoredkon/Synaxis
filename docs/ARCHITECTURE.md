# Arsitektur Synaxis

## Gambaran Umum Sistem

Synaxis adalah sistem sinkronisasi terdistribusi yang mengimplementasikan tiga komponen utama: Distributed Lock Manager, Distributed Queue, dan Distributed Cache dengan protokol MESI coherence.

```mermaid
graph TB
    subgraph Clients
        C1[Client 1]
        C2[Client 2]
        C3[Client N]
    end

    subgraph "Lock Manager Cluster"
        LM1[Lock Manager 1<br/>Raft Leader]
        LM2[Lock Manager 2<br/>Follower]
        LM3[Lock Manager 3<br/>Follower]
        LM1 <--> LM2
        LM1 <--> LM3
        LM2 <--> LM3
    end

    subgraph "Queue Cluster"
        Q1[Queue Node 1<br/>Partitions 0,3]
        Q2[Queue Node 2<br/>Partitions 1,4]
        Q3[Queue Node 3<br/>Partitions 2,5]
        Q1 <--> Q2
        Q2 <--> Q3
        Q1 <--> Q3
    end

    subgraph "Cache Cluster"
        CA1[Cache Node 1<br/>MESI]
        CA2[Cache Node 2<br/>MESI]
        CA3[Cache Node 3<br/>MESI]
        CA1 <-.-> |Bus| CA2
        CA2 <-.-> |Bus| CA3
        CA1 <-.-> |Bus| CA3
    end

    subgraph Infrastructure
        Redis[(Redis<br/>State Store)]
        Prometheus[Prometheus<br/>Metrics]
        Grafana[Grafana<br/>Dashboards]
    end

    C1 --> LM1
    C2 --> Q1
    C3 --> CA1

    LM1 --> Redis
    Q1 --> Redis
    CA1 --> Redis

    LM1 -.-> Prometheus
    Q1 -.-> Prometheus
    CA1 -.-> Prometheus
    Prometheus --> Grafana
```

---

## Arsitektur Komponen

### 1. Distributed Lock Manager

```mermaid
stateDiagram-v2
    [*] --> FOLLOWER
    FOLLOWER --> CANDIDATE: Election Timeout
    CANDIDATE --> LEADER: Majority Votes
    CANDIDATE --> FOLLOWER: Higher Term
    LEADER --> FOLLOWER: Higher Term
    LEADER --> LEADER: Heartbeat
```

**Fitur Utama:**
- **Raft Consensus**: Leader election, log replication
- **Tipe Lock**: Shared (read) dan Exclusive (write)
- **Deadlock Detection**: Analisis wait-for graph
- **Fencing Tokens**: Mencegah stale locks

### 2. Distributed Queue

```mermaid
flowchart LR
    subgraph Producers
        P1[Producer 1]
        P2[Producer 2]
    end

    subgraph "Consistent Hash Ring"
        CH[Key → Partition<br/>Mapping]
    end

    subgraph Partitions
        Part0[Partition 0]
        Part1[Partition 1]
        Part2[Partition 2]
    end

    subgraph "Consumer Groups"
        CG1[Consumer Group A]
        CG2[Consumer Group B]
    end

    P1 --> CH
    P2 --> CH
    CH --> Part0
    CH --> Part1
    CH --> Part2
    Part0 --> CG1
    Part1 --> CG1
    Part2 --> CG2
```

**Fitur Utama:**
- **Consistent Hashing**: Distribusi merata dengan virtual nodes
- **At-Least-Once Delivery**: Acknowledgment dengan retry
- **Consumer Groups**: Partition assignment dan rebalancing
- **Dead Letter Queue**: Penanganan pesan gagal

### 3. Cache Coherence (MESI)

```mermaid
stateDiagram-v2
    M: Modified
    E: Exclusive
    S: Shared
    I: Invalid

    I --> E: PrRd (no other cache)
    I --> S: PrRd (other cache has copy)
    E --> M: PrWr
    E --> S: BusRd
    S --> I: BusRdX / BusUpgr
    M --> S: BusRd (flush)
    M --> I: BusRdX (flush)
    S --> M: PrWr + BusUpgr
```

**Bus Transactions:**
| Transaction | Deskripsi |
|------------|-----------|
| BusRd | Read request, shared access |
| BusRdX | Read dengan intent to modify |
| BusUpgr | Upgrade shared ke exclusive |
| Flush | Write back modified data |

---

## Algoritma Consensus

### Raft (Default)

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: Request
    L->>L: Append to Log
    par Replicate
        L->>F1: AppendEntries
        L->>F2: AppendEntries
    end
    F1-->>L: Success
    F2-->>L: Success
    L->>L: Commit (majority)
    L-->>C: Response
```

### PBFT (Opsional)

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Primary
    participant R1 as Replica 1
    participant R2 as Replica 2
    participant R3 as Replica 3

    C->>P: Request
    P->>R1: Pre-Prepare
    P->>R2: Pre-Prepare
    P->>R3: Pre-Prepare
    par Prepare Phase
        R1->>R2: Prepare
        R1->>R3: Prepare
        R2->>R1: Prepare
        R2->>R3: Prepare
        R3->>R1: Prepare
        R3->>R2: Prepare
    end
    Note over R1,R3: 2f+1 Prepares
    par Commit Phase
        R1->>R2: Commit
        R1->>R3: Commit
        R2->>R1: Commit
        R2->>R3: Commit
        R3->>R1: Commit
        R3->>R2: Commit
    end
    Note over R1,R3: 2f+1 Commits
    R1-->>C: Reply
```

**Toggle via environment:**
```env
CONSENSUS_TYPE=raft   # Default
CONSENSUS_TYPE=pbft   # Byzantine fault tolerant
```

---

## Alur Data

### Lock Acquisition

```mermaid
sequenceDiagram
    participant C as Client
    participant LM as Lock Manager (Leader)
    participant R as Raft Log

    C->>LM: acquire_lock(resource, EXCLUSIVE)
    LM->>LM: Check deadlock
    LM->>R: Propose command
    R->>R: Replicate to followers
    R-->>LM: Committed
    LM-->>C: LockHandle
```

### Message Publishing

```mermaid
sequenceDiagram
    participant P as Producer
    participant Q as Queue Node
    participant CH as Consistent Hash
    participant Part as Partition

    P->>Q: publish(topic, payload, key)
    Q->>CH: get_partition(key)
    CH-->>Q: partition_id
    Q->>Part: append(message)
    Part-->>Q: offset
    Q-->>P: message_id
```

---

## Struktur Direktori

```
src/
├── nodes/
│   ├── base_node.py      # Abstract base dengan MessageBus, FailureDetector
│   ├── lock_manager.py   # Raft + Lock management
│   ├── queue_node.py     # Consistent hashing + Partitions
│   └── cache_node.py     # Protokol MESI + LRU
├── consensus/
│   ├── raft.py           # Leader election, log replication
│   └── pbft.py           # Byzantine fault tolerance
├── communication/
│   ├── message_passing.py # ormsgpack + aiohttp transport
│   └── failure_detector.py # Heartbeat-based detection
└── utils/
    ├── config.py         # Environment configuration
    ├── metrics.py        # Prometheus metrics
    └── consistent_hash.py # Virtual nodes hashing
```

---

## Technology Stack

| Komponen | Teknologi |
|----------|-----------|
| Bahasa | Python 3.12+ |
| Async | asyncio, aiohttp |
| Serialization | ormsgpack (binary), orjson (JSON) |
| State Store | Redis |
| Metrics | Prometheus + Grafana |
| Container | Docker, Docker Compose |
