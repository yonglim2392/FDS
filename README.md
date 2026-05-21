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

## 🚀 Core Engineering Challenges & Architecture Decisions

### 1. Low-Latency Online Feature Store Architecture

#### **Problem** 
* 실시간 결제 승인 시점마다 '최근 10분간의 실시간 거래 빈도'와 '최근 30일간의 평균 결제 행동 패턴'을 조합해야 했습니다. 이를 매 요청마다 RDB Join으로 처리할 경우 디스크 I/O 병목 및 네트워크 RTT 증가로 인해 실시간 승인 SLA(50ms)를 만족할 수 없었습니다.

#### **Solution** 
* 람다 아키텍처 기반으로 연산 레이어를 분리했습니다.
  - `Spark Structured Streaming` → 실시간 10분 윈도우 집계
  - `Airflow Batch Job` → 30일 행동 프로필 계산
  - `Redis` → Unified Online Feature Store

#### **Impact** 
* 최종적으로 추론 엔진은 Redis에서 모든 다차원 피처를 $O(1)$ 레이턴시에 조회하며, 실시간 승인 요청당 평균 **1ms 미만(0.793ms)의 Feature Retrieval Latency**를 달성했습니다.

---

## 2. Disk I/O Isolation with Producer-Consumer Pattern

#### **Problem** 
* 실시간 추론 이후 원천 로그 및 추론 결과를 PostgreSQL에 저장해야 했으나, 동기식 DB Write 구조에서는 다음 문제가 발생했다.
  - Kafka Consumer Lag 증가
  - DB Connection Pool 고갈
  - Disk Flush Wait
  - End-to-End Latency 폭증

#### **Solution** 
* Inference Engine 내부에 다음 구조를 도입하였다.
  - `Bounded In-Memory Queue`
  - `Background Daemon Worker`
  - Producer-Consumer Async Pipeline
    
#### **Impact** 
* 실시간 추론 루프는 메모리 큐에 즉시 결과를 Enqueue한 뒤 다음 카프카 메시지를 처리하며, 별도의 백그라운드 워커가 비동기적으로 DB에 적재합니다.
* 이를 통해 무거운 Disk I/O가 메인 추론 경로(Critical Path)에 미치는 영향을 0%로 통제하는 완전한 I/O Decoupling 구조를 완성했습니다.

* **💡 Architecture Trade-off & Production Next Step:** 단일 노드 제약상 현재 In-Memory Queue를 사용하여 I/O 격리를 구현했으나, 인퍼런스 노드 장애 시 큐 데이터 유실(Data Loss) 리스크가 존재합니다. 실제 프로덕션 도입 시에는 이를 **Kafka Result Topic** 또는 **Redis Stream** 기반의 외부 비동기 큐로 분리하여 영속성(Durability)과 내결함성을 동시에 확보하는 구조로 확장할 수 있도록 설계했습니다.
---

## 3. OOM Prevention & Idempotent Batch Reprocessing

#### **Problem** 
* 수천만 건 규모의 장기 거래 로그 집계 시 전체 로딩(`fetchall()`) 구조는 메모리 고갈(OOM)을 유발하였다.
* 또한 NOW() 기반 비결정성 쿼리는 Backfill/Reprocessing 시 데이터 정합성을 깨뜨리는 문제가 있었다.

#### **Solution** 
* Memory-Safe Batch Aggregation
  - PostgreSQL `Server-side Cursor`
  - `fetchmany(5000)` Chunk Streaming

* Idempotency
  - Airflow `execution_date`(`{{ ds }}`) 기반 논리적 시간 파라미터를 쿼리에 주입하였다.

    ```sql
    WHERE transaction_date < {{ ds }}
    ```

---

## 4. Fault-Tolerant Distributed Streaming

#### **Problem** 
* 분산 스트리밍 노드 장애 발생 시 다음 문제가 존재했다.
  - Window Aggregation State Loss
  - Duplicate Processing
  - Streaming Recovery Failure
* 또한 Redis Connection을 Row 단위로 생성할 경우 네트워크 오버헤드로 처리량이 급격히 감소하였다.

#### **Solution** 
* Fault Tolerance
  - park Structured Streaming에 `.option("checkpointLocation", "...")` 기반 상태 영속화를 적용하여 노드 크래시 발생 시에도 Exactly-Once 수준의 스트리밍 상태 자가 복원력을 확보했습니다.
    ```python
    .option("checkpointLocation", "/tmp/fds-checkpoint")
    ```

* Connection Optimization
  -`foreachPartition` 패턴을 통해 Partition 단위로 Redis Connection을 풀링(Reuse)함으로써 네트워크 오버헤드를 최소화하고 초당 처리량(Throughput)을 극대화했습니다.

* **Action & Result:** Spark Structured Streaming의 결함 허용(Fault Tolerance)을 보장하기 위해 `.option("checkpointLocation", "...")` 레이어를 도입하여 메타데이터와 오프셋을 강제 영속화했습니다. 노드 재시작 시 At-Least-Once로 재처리되지만, Sink 타겟인 **Redis의 `HSET` 연산이 멱등성(Idempotency)을 보장하므로 시스템 전체적으로는 상태 유실이나 중복 적재가 없는 Effectively-Once Semantics를 완벽하게 구현**했습니다.
* 
---

## 🏗️ System Architecture
<img width="1280" height="1734" alt="test drawio (3)" src="https://github.com/user-attachments/assets/dedc7275-d543-4c1d-8f66-905b9bf32431" />

---

## ⚡ Performance Benchmark

실제 운영 환경과 동일한 부하 스트레스 상황을 모사하기 위해 데이터 생성기의 네트워크 대기를 전면 제거한 **최대 하중(No-Sleep Maximum Throughput)** 상태에서 측정한 레이턴시 프로파일링 결과입니다.

- **Test Target:** 1,000 Continuous Transactions **(대기 시간 없는 순간 스파이크 하중 모사 샘플링)**
- **Profiling Tool:** Microsecond Precision (`time.perf_counter()`)

| Pipeline Stage | Mean | p95 | p99 |
|---|---:|---:|---:|
| Total Inference Pipeline | 6.312 ms | 8.709 ms | 11.715 ms |
| Redis Feature Retrieval | 0.793 ms | 1.136 ms | - |
| ML Inference | 5.519 ms | 7.702 ms | - |

<details>
<summary>Benchmark Result Image</summary>

![System Architecture](https://github.com/user-attachments/assets/46675ea8-9f57-4ab7-856b-03583caa7d10)

</details>


---

## 🏁 Key Engineering Outcomes

### ✅ Financial SLA Compliance & Business Impact
결제 승인 과정에서 FDS 판정 지연이 50ms를 초과할 경우, PG(Payment Gateway)사와의 통신 타임아웃이 발생하거나 사용자의 결제 이탈률이 급증하는 치명적인 비즈니스 손실이 발생합니다.
본 아키텍처는 최대 하중 상태에서도 **p99 기준 11.715ms의 안정적인 추론 성능**을 확보했습니다. 이는 결제 시스템의 병목을 제로(0) 수준으로 방어하면서도 고도화된 머신러닝 사기 탐지 방어막을 무중단으로 운영할 수 있음을 증명합니다.

### ✅ ML Serving Bottleneck Identification
프로파일링 결과 전체 레이턴시의 약 87%가 인프라 I/O가 아닌 ML Inference 단계에서 발생함을 식별했습니다. 이를 통해 향후 시스템 고도화의 타겟이 네트워크가 아닌 **ML 모델 최적화(LightGBM 전환 또는 ONNX Runtime 가속)**에 있음을 데이터 기반으로 도출했습니다.

---

## 📊 Real-Time Monitoring Dashboard

텍스트 로그의 한계를 벗어나 관제 센터 환경의 시각화 모니터링 생태계를 입증하기 위해 SQLAlchemy Connection Pooling 기반 고속 대시보드를 구축했습니다.

<img width="1757" height="797" alt="image" src="https://github.com/user-attachments/assets/013d724c-b959-4df4-ac03-eb52fc1294db" />

- **Real-Time Metrics:** 최근 500건 기준 총 트랜잭션, 승인/차단 건수, 실시간 탐지율 연산
- **Dynamic Visualization:** Plotly 차트를 활용한 1초 주기 실시간 승인 분율 및 고위험 가맹점 분류 렌더링
- **Fraud Monitoring Console:** 사기 판정된 유저의 ID, 금액, 기기 정보, ML 사기 확률(%) 실시간 추적 테이블

---

## 🗂️ Project Structure

```text
├── docker/
│   └── docker-compose.yml       # 인프라 컴포넌트 컨테이너 명세
├── data-generator/
│   └── generator.py             # 하중 모드 및 사기 공격 시나리오 트랜잭션 생성기
├── streaming/
│   └── spark_processor.py       # Checkpoint 기반 Spark 분산 윈도우 스트리밍 엔진
├── batch/
│   ├── fds_batch_job.py         # 커서 기반 대용량 멱등성 배치 가공 잡
│   └── airflow_dags/
│       └── fds_dag.py           # execution_date 매핑형 Airflow DAG 
├── model/
│   ├── train.py                 # 스키마 동기화 및 람다 피처 ML 학습 스크립트
│   ├── inference.py             # Producer-Consumer 큐 기반 비동기 적재 인라인 추론 엔진
│   ├── benchmark.py             # 마이크로초 정밀 성능 측정 프로파일러
│   └── fds_model.pkl            
└── dashboard/
    └── app.py                   # 실시간 FDS 관제 대시보드 웹 앱
```

---

## 🛠️ Technology Stack

- Data Streaming: Apache Kafka, Spark Structured Streaming
- Storage: PostgreSQL (Operational Data Store, ODS), Redis (Online Feature Store)
- ML & Data Processing: Python, scikit-learn, Pandas
- Orchestration: Apache Airflow
- Infrastructure: Docker, Docker Compose
- Monitoring: Streamlit, Plotly
- 
---

## ▶️ Quick Start

본 프로젝트는 분산 환경 모사를 위해 컨테이너 기반으로 작성되었으며, 원활한 파이프라인 구동을 위해 다음 환경을 권장합니다.

* **OS:** Ubuntu 22.04 LTS (WSL2 환경 테스트 완료)
* **Compute Minimum Spec:** 4 Cores, 8GB RAM (Kafka & Spark 메모리 할당용)
* **Engine & Runtime:**
  * Python 3.11+
  * Docker Engine 24.0+ & Docker Compose v2.0+
* **Dependencies:** `requirements.txt` 참조
---

### 1. Start Infrastructure
```bash
# 컨테이너 인프라 기동
cd docker
docker compose up -d
cd ..

# 파이썬 의존성 패키지 설치
pip install -r requirements.txt
```

### 2. Train Initial ML Model (Cold-Start Prevention)
```bash
python model/train.py
```

### 3. Start Spark Streaming Pipeline
```bash
python streaming/spark_processor.py
```

### 4. Execute Batch Aggregation (Sync Baseline Features)
```bash
python batch/fds_batch_job.py
```

### 5. Start Real-Time Inference Engine
```bash
python model/inference.py
```

### 6. Run Generator & Dashboard (Observability)
```bash
# Terminal A: 트랜잭션 스트레스 주입 시작
python data-generator/generator.py

# Terminal B: 실시간 시각화 브라우저 렌더링
streamlit run dashboard/app.py
```

---

## 🔥 Troubleshooting & Lessons Learned

개발 및 아키텍처 고도화 과정에서 마주친 치명적인 분산 시스템 문제들과 해결 과정입니다.

### 1. Spark Streaming 노드 크래시 시 인메모리 윈도우 상태(State) 증발 문제
* **Issue:** 로컬 인프라 하중 테스트 중 Spark 워커 노드가 OOM으로 크래시된 후 재시작되었을 때, 기존에 연산 중이던 10분 슬라이딩 윈도우의 카운트와 누적액 메모리가 전부 증발하여 탐지 정합성이 깨지는 현상을 발견했습니다.
* **Action & Result:** Spark Structured Streaming의 결함 허용(Fault Tolerance)을 보장하기 위해 `.option("checkpointLocation", "...")` 레이어를 도입하여 HDFS/로컬 디스크에 메타데이터와 오프셋을 강제 영속화(Persist)했습니다. 이를 통해 노드가 언제 죽더라도 정확히 중단된 지점(Exactly-Once Semantics)부터 상태를 자가 복구하도록 파이프라인의 생존성을 확보했습니다.

### 2. Lambda Architecture 환경에서의 ML Feature Schema Mismatch 에러
* **Issue:** 실시간 스트리밍 피처(10분)와 배치 피처(30일)를 융합하여 추론 엔진에 주입하는 과정에서, 오프라인 모델 학습 시점의 Feature 차원(Dimension)과 실시간 온라인 Serving 시점의 데이터프레임 컬럼 순서가 미세하게 어긋나 런타임 에러(`ValueError: feature names mismatch`)가 발생했습니다.
* **Action & Result:** 모델 서빙 레이어에서 입력 데이터프레임의 스키마 배열을 오프라인 `train.py`와 동일한 7-Dimensional Vector로 강제 정렬(`input_df = input_df[feature_order]`)하는 전처리 파이프라인을 구축했습니다. 이를 통해 람다 아키텍처 하에서 오프라인 환경과 온라인 환경의 피처 정합성(Feature Consistency)을 100% 동기화하는 데 성공했습니다.

---

## 🎯 Future Improvements
- ONNX Runtime 기반 추론 가속
- LightGBM 기반 고속 모델 전환
- Kafka Multi-Broker Cluster 확장
- Kubernetes 기반 Auto Scaling
- Real-Time Feature Drift Detection
- CDC 기반 Online Feature Synchronization
- Prometheus + Grafana Observability Stack 구축
