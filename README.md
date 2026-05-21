# README.md

# 🛡️ Real-Time Fraud Detection System (Lambda Architecture FDS)

> Distributed Real-Time Fraud Detection Platform with Streaming Feature Engineering, Online Feature Store, Fault-Tolerant Stream Processing, and Low-Latency ML Inference Architecture

---

## 📌 Overview

본 프로젝트는 초당 수천 건 규모의 금융 트랜잭션 스트림 환경에서 이상 거래(Fraud)를 실시간 탐지·차단하기 위한 분산형 FDS(Fraud Detection System) 플랫폼이다.

단순 이벤트 적재 수준의 스트리밍 예제가 아니라, 실제 금융권 환경에서 요구되는 다음 핵심 엔지니어링 요구사항을 중심으로 설계되었다.

- 50ms 이하의 실시간 추론 SLA
- Fault-Tolerant Distributed Streaming
- Idempotent Batch Reprocessing
- Online/Offline Feature Consistency
- Disk I/O Isolation
- Consumer Lag Suppression
- OOM-Free Large Scale Batch Aggregation
- Low-Latency Online Feature Enrichment

특히 Lambda Architecture 기반으로 Batch Layer와 Speed Layer를 분리하고, Redis Online Feature Store를 중심으로 실시간 Feature Enrichment 구조를 구축하여, 스트리밍 집계와 장기 행동 프로필을 동시에 활용하는 하이브리드 ML 추론 파이프라인을 구현하였다.

---

# 🚀 Core Engineering Challenges & Architecture Decisions

## 1. Low-Latency Online Feature Store Architecture

### Problem

실시간 결제 승인 시점마다 다음 정보를 조합해야 했다.

- 최근 10분간의 실시간 거래 빈도
- 최근 30일간의 평균 결제 행동 패턴
- 디바이스·가맹점 기반 위험 프로필

이를 매 요청마다 RDB Join으로 처리할 경우 디스크 I/O 병목 및 네트워크 RTT 증가로 인해 실시간 승인 SLA를 만족할 수 없었다.

### Solution

람다 아키텍처 기반으로 Batch Layer와 Streaming Layer를 분리하였다.

- Spark Structured Streaming → 실시간 10분 윈도우 집계
- Airflow Batch Job → 30일 행동 프로필 계산
- Redis → Unified Online Feature Store

최종적으로 추론 엔진은 Redis에서 모든 피처를 O(1)에 조회하며, 실시간 승인 요청당 평균 1ms 미만의 Feature Retrieval Latency를 달성하였다.

---

## 2. Disk I/O Isolation with Producer-Consumer Pattern

### Problem

실시간 추론 이후 원천 로그 및 추론 결과를 PostgreSQL에 저장해야 했으나, 동기식 DB Write 구조에서는 다음 문제가 발생했다.

- Kafka Consumer Lag 증가
- DB Connection Pool 고갈
- Disk Flush Wait
- End-to-End Latency 폭증

### Solution

Inference Engine 내부에 다음 구조를 도입하였다.

- Bounded In-Memory Queue
- Background Daemon Worker
- Producer-Consumer Async Pipeline

실시간 추론 루프는 메모리 큐에 즉시 Enqueue 후 다음 메시지를 처리하며, 별도 백그라운드 워커가 비동기적으로 PostgreSQL에 적재한다.

이를 통해 Disk I/O가 메인 추론 경로에 영향을 주지 않는 완전한 I/O Decoupling 구조를 구현하였다.

---

## 3. OOM Prevention & Idempotent Batch Reprocessing

### Problem

수천만 건 규모의 장기 거래 로그 집계 시 전체 로딩(fetchall()) 구조는 메모리 고갈(OOM)을 유발하였다.

또한 NOW() 기반 비결정성 쿼리는 Backfill/Reprocessing 시 데이터 정합성을 깨뜨리는 문제가 있었다.

### Solution

#### Memory-Safe Batch Aggregation

- PostgreSQL Server-side Cursor
- fetchmany(5000) Chunk Streaming

을 적용하여 메모리 사용량을 상한선 내로 제한하였다.

#### Fully Idempotent Batch Design

Airflow execution_date 기반 논리적 시간 파라미터를 쿼리에 주입하였다.

```sql
WHERE transaction_date < {{ ds }}
```

이를 통해 언제 재실행하더라도 동일한 결과를 보장하는 완전한 멱등성을 확보하였다.

## 4. Fault-Tolerant Distributed Streaming

### Problem

분산 스트리밍 노드 장애 발생 시 다음 문제가 존재했다.

- Window Aggregation State Loss
- Duplicate Processing
- Streaming Recovery Failure

또한 Redis Connection을 Row 단위로 생성할 경우 네트워크 오버헤드로 처리량이 급격히 감소하였다.

### Solution

#### - Streaming Fault Tolerance

Spark Structured Streaming의 Checkpoint 기반 상태 영속화를 적용하였다.

```python
.option("checkpointLocation", "/tmp/fds-checkpoint")
```

노드 장애 발생 시 Window State 복구 및 Exactly-Once 수준의 스트리밍 상태 복원을 가능하게 했다.

#### - Partition-Level Redis Optimization

foreachPartition 패턴을 통해 Partition 단위 Redis Connection Reuse 구조를 적용하였다.

이를 통해 Redis 네트워크 오버헤드를 최소화하고 처리량(Throughput)을 크게 향상시켰다.

# 🏗️ System Architecture

```text
[ Transaction Generator ]
        │
        ▼
[ Apache Kafka ]
        │
        ├──────────────────────────────┐
        ▼                              ▼
[ Spark Structured Streaming ]   [ ML Inference Engine ]
        │                              │
        │                              ├─ Redis Online Feature Join
        │                              ├─ ML Probability Inference
        │                              ├─ Fraud Threshold Decision
        ▼                              │
[ Redis Feature Store ] <──────────────┘
        ▲
        │
[ Airflow Batch Layer ]
        │
        ▼
[ PostgreSQL Data Lake ]
```

# ⚡ Performance Benchmark

## Stress Test Environment

- No-Sleep Maximum Throughput Mode
- 1,000 Continuous Transactions
- Microsecond Precision Profiling
- `time.perf_counter()` 기반 측정

| Pipeline Stage | Mean | p95 | p99 |
|---|---:|---:|---:|
| Total Inference Pipeline | 6.312 ms | 8.709 ms | 11.715 ms |
| Redis Feature Retrieval | 0.793 ms | 1.136 ms | - |
| ML Inference | 5.519 ms | 7.702 ms | - |

---

# 🏁 Key Engineering Outcomes

## ✅ Financial SLA Compliance

p99 기준 11.715ms의 안정적인 추론 성능을 확보하여 금융권 실시간 승인 시스템의 일반적인 SLA 기준인 50ms 이하를 안정적으로 만족하였다.

---

## ✅ Complete I/O Decoupling

Redis 기반 Online Feature Join과 비동기 Queue 적재 구조를 통해 PostgreSQL Disk Write 비용이 실시간 승인 레이턴시에 영향을 주지 않도록 설계하였다.

---

## ✅ ML Serving Bottleneck Identification

프로파일링 결과 전체 레이턴시의 약 87%가 ML Inference 단계에서 발생함을 확인하였다.

### Future Optimization Directions

- LightGBM Migration
- ONNX Runtime Acceleration
- Native C++ Inference Serving

# 📊 Real-Time Monitoring Dashboard

### Real-Time Metrics
- Total Transactions
- Approved / Blocked Counts
- Fraud Detection Rate
- Real-Time Throughput
  
### Dynamic Visualization
- Fraud Ratio Pie Chart
- High-Risk Merchant Analysis
- Live Detection Trend Graph

### Fraud Monitoring Console
실시간 차단된 거래에 대해 다음 정보를 추적 가능하다.

- Transaction ID
- User ID
- Device ID
- Merchant Category
- Fraud Probability Score

# 🗂️ Project Structure

```text
├── docker/
│   └── docker-compose.yml

├── data-generator/
│   └── generator.py

├── streaming/
│   └── spark_processor.py

├── batch/
│   ├── fds_batch_job.py
│   └── airflow_dags/
│       └── fds_dag.py

├── model/
│   ├── train.py
│   ├── inference.py
│   ├── benchmark.py
│   └── fds_model.pkl

├── dashboard/
│   └── app.py
```

# 🛠️ Technology Stack

### Data Streaming
- Apache Kafka
- Spark Structured Streaming

### Storage
- PostgreSQL
- Redis

### ML & Data Processing
- Python
- scikit-learn
- Pandas
- NumPy

### Orchestration
- Apache Airflow

### Infrastructure
- Docker
- Docker Compose

### Monitoring
- Streamlit
- Plotly

# ▶️ Quick Start

### 1. Start Infrastructure
```bash
cd docker
docker compose up -d
```

### 2. Train Initial Model
```bash
python model/train.py
```

### 3. Start Streaming Pipeline
```bash
python streaming/spark_processor.py
```

### 4. Execute Batch Aggregation
```bash
python batch/fds_batch_job.py
```

### 5. Start Real-Time Inference Engine
```bash
python model/inference.py
```

### 6. Run Generator & Dashboard
```bash
# Terminal A
python data-generator/generator.py

# Terminal B
streamlit run dashboard/app.py
```

# 🎯 Future Improvements
- ONNX Runtime 기반 추론 가속
- LightGBM 기반 고속 모델 전환
- Kafka Multi-Broker Cluster 확장
- Kubernetes 기반 Auto Scaling
- Real-Time Feature Drift Detection
- CDC 기반 Online Feature Synchronization
- Prometheus + Grafana Observability Stack 구축
